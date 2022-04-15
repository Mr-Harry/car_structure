#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>

typedef struct w_t
{
    char word[1024];
    char tag[1024];
} WordTag;

void strjoin(char **dest, char *str[], int start, int end)
{
    int len = 0,len_sub;
    for (int i = start; i < end; i++){
        len += strlen(*(str + i));
    }
    char *dst = (char *)malloc(sizeof(char) * (len + 1));
    *dest = dst;
    for (int i = start; i < end; i++){
        len_sub = strlen(*(str + i));
        memcpy(dst, *(str + i), len_sub);
        dst += len_sub;
    }
    *dst = '\0';
}

WordTag *get_words_targs_bios(char *str[], char *label[], int num)
{
    int word_id = 0;
    bool st = false;
    char l2[1024];
    int i;
    WordTag *wt = (WordTag *)malloc(sizeof(WordTag) * 50);
    int wti = 0;
    for (i = 0; i < num; i++)
    {
        if (!strcmp(*(label + i), "<pad>"))
            break;
        if (strcmp(*(label + i), "O"))
        {
            char *_l1;
            char *_l2;
            char _l[1024];
            strcpy(_l, *(label + i));
            _l1 = strtok(_l, "-");
            _l2 = strtok(NULL, "-");
            if (!strcmp(_l1, "B"))
            {
                if (st)
                {
                    char *dest;
                    strjoin(&dest, str, word_id, i);
                    strcpy(wt[wti].word, dest);
                    strcpy(wt[wti].tag, l2);
                    wti += 1;
                    free(dest);
                    word_id = i;
                    strcpy(l2, _l2);
                }
                else
                {
                    st = true;
                    word_id = i;
                    strcpy(l2, _l2);
                }
            }
            if (!strcmp(_l1, "I"))
            {
                if (st)
                {
                    if (strcmp(_l2, l2))
                    {
                        char *dest;
                        strjoin(&dest, str, word_id, i);
                        strcpy(wt[wti].word, dest);
                        strcpy(wt[wti].tag, l2);
                        wti += 1;
                        free(dest);
                        word_id = i;
                        strcpy(l2, _l2);
                        st = true;
                    }
                }
                else
                {
                    st = true;
                    word_id = i;
                    strcpy(l2, _l2);
                }
            }
        }
        else if (st)
        {

            char *dest;
            strjoin(&dest, str, word_id, i);
            strcpy(wt[wti].word, dest);
            strcpy(wt[wti].tag, l2);
            wti += 1;
            free(dest);
            st = false;
        }
    }
    if (st)
    {
        char *dest;
        strjoin(&dest,str, word_id, i);
        strcpy(wt[wti].word, dest);
        strcpy(wt[wti].tag, l2);
        wti += 1;
        free(dest);
    }
    strcpy(wt[wti].word, "E");
    return wt;
}
void freeme(void * p){
    // printf("freeing address: %p\n", p);
    free(p);
}





int main()
{
    char *str[] = {"你", "好", "啊", "哈", "，", "7", "8"};
    char *label[] = {"B-A你", "I-A你", "I-A你", "I-A你", "O", "B-B世界", "I-B世界"};
    WordTag *wt;
    wt = get_words_targs_bios(str, label, 7);
    for (int i = 0; i < 128; i++)
    {
        if (strcmp(wt[i].word, "E") == 0)
            break;
        printf("%s||%s,===\n", wt[i].word, wt[i].tag);
    }
    freeme(wt);

    return 0;
}
