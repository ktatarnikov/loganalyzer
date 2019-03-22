
import re
import os
import numpy as np
import pandas as pd
import hashlib
from datetime import datetime

try:
    # Python 2
    xrange
except NameError:
    # Python 3, xrange is now named range
    xrange = range


class Logcluster:
    def __init__(self, level, logTemplate='', logIDL=None):
        self.logTemplate = logTemplate
        if logIDL is None:
            logIDL = []
        self.logIDL = logIDL
        self.level = level


class Node:
    def __init__(self, childD=None, depth=0, digitOrtoken=None):
        if childD is None:
            childD = dict()
        self.childD = childD
        self.depth = depth
        self.digitOrtoken = digitOrtoken


class LogParser:
    def __init__(self, outdir='./result/', depth=4, st=0.4, maxChild=100):
        """
        Attributes
        ----------
            rex : regular expressions used in preprocessing (step1)
            path : the input path stores the input log file name
            depth : depth of all leaf nodes
            st : similarity threshold
            maxChild : max number of children of an internal node
            logName : the name of the input file containing raw log messages
            savePath : the output path stores the file containing structured logs
        """
        self.depth = depth - 2
        self.st = st
        self.maxChild = maxChild
        self.savePath = outdir
        self.df_log = None
        self.rootNode = Node()
        self.logCluL = []

    def hasNumbers(self, s):
        return any(char.isdigit() for char in s)

    def treeSearch(self, rn, seq):
        retLogClust = None

        seqLen = len(seq)
        if seqLen not in rn.childD:
            return retLogClust

        parentn = rn.childD[seqLen]

        currentDepth = 1
        for token in seq:
            if currentDepth >= self.depth or currentDepth > seqLen:
                break

            if token in parentn.childD:
                parentn = parentn.childD[token]
            elif '<*>' in parentn.childD:
                parentn = parentn.childD['<*>']
            else:
                return retLogClust
            currentDepth += 1

        logClustL = parentn.childD

        retLogClust = self.fastMatch(logClustL, seq)

        return retLogClust

    def addSeqToPrefixTree(self, rn, logClust):
        seqLen = len(logClust.logTemplate)
        if seqLen not in rn.childD:
            firtLayerNode = Node(depth=1, digitOrtoken=seqLen)
            rn.childD[seqLen] = firtLayerNode
        else:
            firtLayerNode = rn.childD[seqLen]

        parentn = firtLayerNode

        currentDepth = 1
        for token in logClust.logTemplate:

            #Add current log cluster to the leaf node
            if currentDepth >= self.depth or currentDepth > seqLen:
                if len(parentn.childD) == 0:
                    parentn.childD = [logClust]
                else:
                    parentn.childD.append(logClust)
                break

            #If token not matched in this layer of existing tree.
            if token not in parentn.childD:
                if not self.hasNumbers(token):
                    if '<*>' in parentn.childD:
                        if len(parentn.childD) < self.maxChild:
                            newNode = Node(depth=currentDepth + 1, digitOrtoken=token)
                            parentn.childD[token] = newNode
                            parentn = newNode
                        else:
                            parentn = parentn.childD['<*>']
                    else:
                        if len(parentn.childD)+1 < self.maxChild:
                            newNode = Node(depth=currentDepth+1, digitOrtoken=token)
                            parentn.childD[token] = newNode
                            parentn = newNode
                        elif len(parentn.childD)+1 == self.maxChild:
                            newNode = Node(depth=currentDepth+1, digitOrtoken='<*>')
                            parentn.childD['<*>'] = newNode
                            parentn = newNode
                        else:
                            parentn = parentn.childD['<*>']

                else:
                    if '<*>' not in parentn.childD:
                        newNode = Node(depth=currentDepth+1, digitOrtoken='<*>')
                        parentn.childD['<*>'] = newNode
                        parentn = newNode
                    else:
                        parentn = parentn.childD['<*>']

            #If the token is matched
            else:
                parentn = parentn.childD[token]

            currentDepth += 1

    #seq1 is template
    def seqDist(self, seq1, seq2):
        assert len(seq1) == len(seq2)
        simTokens = 0
        numOfPar = 0

        for token1, token2 in zip(seq1, seq2):
            if token1 == '<*>':
                numOfPar += 1
                continue
            if token1 == token2:
                simTokens += 1

        retVal = float(simTokens) / len(seq1)

        return retVal, numOfPar


    def fastMatch(self, logClustL, seq):
        retLogClust = None

        maxSim = -1
        maxNumOfPara = -1
        maxClust = None

        for logClust in logClustL:
            curSim, curNumOfPara = self.seqDist(logClust.logTemplate, seq)
            if curSim>maxSim or (curSim==maxSim and curNumOfPara>maxNumOfPara):
                maxSim = curSim
                maxNumOfPara = curNumOfPara
                maxClust = logClust

        if maxSim >= self.st:
            retLogClust = maxClust

        return retLogClust

    def getTemplate(self, seq1, seq2):
        assert len(seq1) == len(seq2)
        retVal = []

        i = 0
        for word in seq1:
            if word == seq2[i]:
                retVal.append(word)
            else:
                retVal.append('<*>')

            i += 1

        return retVal

    def outputResult(self, df_log, logClustL, logname):
        if not os.path.exists(self.savePath):
            os.makedirs(self.savePath)

        log_templates = [0] * df_log.shape[0]
        log_templateids = [0] * df_log.shape[0]
        log_templatelevels = [0] * df_log.shape[0]
        df_events = []
        for logClust in logClustL:
            template_str = ' '.join(logClust.logTemplate)
            occurrence = len(logClust.logIDL)
            level = logClust.level
            template_id = hashlib.md5(template_str.encode('utf-8')).hexdigest()[0:8]
            for logID in logClust.logIDL:
                logID -= 1
                log_templates[logID] = template_str
                log_templateids[logID] = template_id
                log_templatelevels[logID] = level
            df_events.append([template_id, level, template_str, occurrence])

        df_event = pd.DataFrame(df_events, columns=['EventId', 'Level', 'EventTemplate', 'Occurrences'])
        df_log['EventId'] = log_templateids
        df_log['EventTemplate'] = log_templates
        df_log['Level'] = log_templatelevels

        # self.df_log.drop(['Content'], inplace=True, axis=1)
        df_log.to_csv(os.path.join(self.savePath, logname + '_structured.csv'), index=False)


        occ_dict = dict(df_log['EventTemplate'].value_counts())
        levels = df_log[['EventTemplate', 'Level']]
        # counts = levels.groupby(by =['EventTemplate', 'Level']).count()

        level_dict = levels.set_index('EventTemplate').to_dict()['Level']
        df_event = pd.DataFrame()
        df_event['EventTemplate'] = df_log['EventTemplate'].unique()
        df_event['Level'] = df_event['EventTemplate'].map(level_dict)
        df_event['EventId'] = df_event['EventTemplate'].map(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()[0:8])
        df_event['Occurrences'] = df_event['EventTemplate'].map(occ_dict)
        df_event.sort_values(by=['Level'], inplace=True)
        df_event.to_csv(os.path.join(self.savePath, logname + '_templates.csv'), index=False, columns=["EventId", "Level", "EventTemplate", "Occurrences"])


    def printTree(self, node, dep):
        pStr = ''
        for i in range(dep):
            pStr += '\t'

        if node.depth == 0:
            pStr += 'Root'
        elif node.depth == 1:
            pStr += '<' + str(node.digitOrtoken) + '>'
        else:
            pStr += node.digitOrtoken

        print(pStr)

        if node.depth == self.depth:
            return 1
        for child in node.childD:
            self.printTree(node.childD[child], dep+1)

    def add_log(self, log_id, log_message, level):
        matchCluster = self.treeSearch(self.rootNode, log_message)
        #Match no existing log cluster
        if matchCluster is None:
            newCluster = Logcluster(logTemplate=log_message, logIDL=[log_id], level=level)
            self.logCluL.append(newCluster)
            self.addSeqToPrefixTree(self.rootNode, newCluster)
            return "new"
        #Add the new log message to the existing cluster
        else:
            newTemplate = self.getTemplate(log_message, matchCluster.logTemplate)
            matchCluster.logIDL.append(log_id)
            if ' '.join(newTemplate) != ' '.join(matchCluster.logTemplate):
                matchCluster.logTemplate = newTemplate
                matchCluster.level = level
            return "existing"


    def parse(self, df_log):
        start_time = datetime.now()

        count = 0
        for idx, line in df_log.iterrows():
            log_id = line['LineId']
            log_message = line['Content'].strip().split()
            level = line['Level'].strip()

            self.add_log(log_id, log_message, level)

            count += 1
            if count % 1000 == 0 or count == len(df_log):
                print('Processed {0:.1f}% of log lines.'.format(count * 100.0 / len(df_log)))

        print('Parsing done. [Time taken: {!s}]'.format(datetime.now() - start_time))
