import pickle
import time
import os
import queue

class BaseCmd:
    def __init__(self, cmd):
        self.Next = None
        self.cmd = cmd

    def SetNext(self, n):
        self.Next = n

    async def DoAnalysis(self, cmd, **kwargs):
        if cmd == self.cmd:
            await self.Work(**kwargs)
        elif self.Next is not None:
            await self.Next.DoAnalysis(cmd, **kwargs)

    async def Work(self, **kwargs):
        raise NotImplementedError


class CmdAnaly:
    def __init__(self):
        self.CmdList = []

    def add(self, cmd):
        self.CmdList.append(cmd)
        if len(self.CmdList) > 1:
            self.CmdList[len(self.CmdList) - 2].SetNext(
                self.CmdList[len(self.CmdList) - 1])

    async def Analy(self, cmd, kwargs):
        await self.CmdList[0].DoAnalysis(cmd, **kwargs)


