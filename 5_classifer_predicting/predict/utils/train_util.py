from utils.sequence_labeling import classification, f1_score
from mxnet import gluon, nd
import gluonnlp as nlp
import mxnet as mx
from utils.data import PredictDataTransform, BERTforNERTransform


def get_Result(label, pred, tagvocab, lengths=None):
    def i2b(_list, length,is_ids=True):
        _list_ = []
        if length is None:
            length = [None] * len(_list)
        for p, le in zip(_list, length):
            if le is not None:
                if is_ids:
                    pp = tagvocab.to_tokens(p[1:le - 1])
                else:
                    pp = p[1:le - 1]
            else:
                if is_ids:
                    pp = tagvocab.to_tokens(p[1:-1])
                else:
                    pp = p[1:-1]
            pn = []
            for pnp in pp:
                if pnp not in ['<bos>', '<eos>', 'X']:
                    pn.append(pnp)
                else:
                    break
            _list_.append(pn)
        return _list_

    pred_b = i2b(pred, lengths)
    label_b = i2b(label, lengths,is_ids=False)
    res, f1 = classification(label_b, pred_b)
    return res, f1


def forward(data_len, dataloader, net, tagvocab, context, mode='crf',
            loss_function=None):
    outs = []
    labels = []
    datas = []
    valid_lengths = []
    
    ls = 0.0
    if mode == 'softmax':
        for inputs, _, valid_length, label_ids, data, label in dataloader:
            out = net(inputs.astype('float32').as_in_context(context))
            ls += loss_function(
                out,
                label_ids.astype('float32').as_in_context(context)).mean(
                ).asscalar() if loss_function is not None else 0.0
            out = nd.softmax(out).argmax(axis=2).asnumpy().astype('int32').tolist()
            # prob = nd.softmax(out).max(axis=2).asnumpy()
            outs.extend(out.asnumpy().astype('int32').tolist())
            datas.extend(data)
            labels.extend(label)
            valid_lengths.extend(valid_length.asnumpy().astype('int32').tolist())
    if mode == 'crf':
        _outs = nd.ones((data_len, 512), ctx=context)
        i = 0
        _valid_lengths = []
        for inputs, _, valid_length, label_ids, data, label in dataloader:
            j = inputs.shape[0]
            vvars = nd.full((inputs.shape[0], len(tagvocab)),
                            -10000,
                            ctx=context)
            vvars[:, tagvocab['<eos>']] = 0.0
            _, batch_pred, feats = net(inputs.as_in_context(context), vvars)
            ls += loss_function(
                feats,
                label_ids.astype('float32').as_in_context(context)).mean(
                ).asscalar() if loss_function is not None else 0.0
            _outs[i:i + j, :inputs.shape[1]] = batch_pred
            i = i + j
            labels.extend(label)
            datas.extend(data)
            _valid_lengths.append(valid_length)
        valid_lengths.extend(
            nd.concat(*_valid_lengths, dim=0).asnumpy().astype('int32').tolist())
        outs = _outs.asnumpy().astype('int32').tolist()
    if mode == 'distilling_bert':
        _outs = []
        _valid_lengths = []
        for batch_data in dataloader:
            inputs, _, valid_length, label_ids,data, label= batch_data

            out = net(
                inputs.astype('float32').as_in_context(context))

            ls += loss_function(
                out.astype('float32'),
                label_ids.astype('float32').as_in_context(context)).mean(
                ).asscalar() if loss_function is not None else 0.0
            out = nd.softmax(out).argmax(axis=2)
            _outs.append(out)
            labels.extend(label)
            _valid_lengths.append(valid_length)
            datas.extend(data)
        outs.extend(nd.concat(*_outs, dim=0).asnumpy().astype('int32').tolist())
        valid_lengths.extend(
            nd.concat(*_valid_lengths, dim=0).asnumpy().astype('int32').tolist())
    if mode == 'bert':
        _outs = []
        _valid_lengths = []
        for batch_data in dataloader:
            inputs, token_types, valid_length, label_ids, data,label = batch_data

            out = net(
                inputs.astype('float32').as_in_context(context),
                token_types.astype('float32').as_in_context(context),
                valid_length.astype('float32').as_in_context(context))

            ls += loss_function(
                out.astype('float32'),
                label_ids.astype('float32').as_in_context(context)).mean(
                ).asscalar() if loss_function is not None else 0.0
            out = nd.softmax(out).argmax(axis=2)
            _outs.append(out)
            labels.extend(label)
            datas.extend(data)
            _valid_lengths.append(valid_length)
        outs.extend(nd.concat(*_outs, dim=0).asnumpy().astype('int32').tolist())
        valid_lengths.extend(
            nd.concat(*_valid_lengths, dim=0).asnumpy().astype('int32').tolist())
    return ls, outs, labels, valid_lengths, datas


def evaluate(data_len, dataloader, net, tagvocab, context, loss_function, mode='crf'):

    ls, outs, labels, valid_lengths, datas = forward(data_len, dataloader, net, tagvocab,
                                              context, mode, loss_function)
    if valid_lengths == []:
        valid_lengths = None

    return ls / len(dataloader), get_Result(labels, outs, tagvocab, valid_lengths)


def write_file(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        for d in data:
            for s, l in zip(*d):
                f.write('\t'.join([s, l]) + '\n')
            f.write('\n')



def get_dataload(data, vocab, msg_num, tokenizer,batch_size, mode='crf'):
    if mode not in ['bert', 'distilling_bert']:
        tran = PredictDataTransform(vocab=vocab,
                                    num=msg_num,
                                    tokenizer=tokenizer)

    else:
        bert_token = BERTforNERTransform(tokenizer,
                                            vocab, tagvocab)
        tran = PredictDataTransform(vocab=vocab,
                                    num=msg_num,
                                    tokenizer=bert_token,
                                    mode=mode)
    batchify_fn = nlp.data.batchify.Tuple(
        nlp.data.batchify.Stack(),
        nlp.data.batchify.Stack(),
        nlp.data.batchify.Stack(),
        nlp.data.batchify.Stack(),
        nlp.data.batchify.List(), nlp.data.batchify.List())

    data_tran = data.transform(tran)

    if len(data) == 1:
        dataloader = mx.gluon.data.DataLoader(
            data_tran,
            batchify_fn=batchify_fn,
            batch_size=batch_size,
            last_batch='keep')
    else:
        dataloader = mx.gluon.data.DataLoader(
            data_tran,
            batchify_fn=batchify_fn,
            batch_size=batch_size,
            last_batch='keep',
            # pin_memory=True,
            num_workers=10)

    return dataloader
