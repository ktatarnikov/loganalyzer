
import re
import os
import sys
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

class LCSObject:
    """ Class object to store a log group with the same template
    """
    def __init__(self, level, logTemplate='', logIDL=[]):
        self.logTemplate = logTemplate
        self.logIDL = logIDL
        self.level = level


class Node:
    """ A node in prefix tree data structure
    """
    def __init__(self, token='', templateNo=0):
        self.logClust = None
        self.token = token
        self.templateNo = templateNo
        self.childD = dict()


class LogParser:
    """ LogParser class

    Attributes
    ----------
        path : the path of the input file
        logName : the file name of the input file
        savePath : the path of the output file
        tau : how much percentage of tokens matched to merge a log message
    """
    def __init__(self, outdir='./result/', tau=0.5):
        self.savePath = outdir
        self.tau = tau

    def LCS(self, seq1, seq2):
        lengths = [[0 for j in range(len(seq2)+1)] for i in range(len(seq1)+1)]
        # row 0 and column 0 are initialized to 0 already
        for i in range(len(seq1)):
            for j in range(len(seq2)):
                if seq1[i] == seq2[j]:
                    lengths[i+1][j+1] = lengths[i][j] + 1
                else:
                    lengths[i+1][j+1] = max(lengths[i+1][j], lengths[i][j+1])

        # read the substring out from the matrix
        result = []
        lenOfSeq1, lenOfSeq2 = len(seq1), len(seq2)
        while lenOfSeq1!=0 and lenOfSeq2 != 0:
            if lengths[lenOfSeq1][lenOfSeq2] == lengths[lenOfSeq1-1][lenOfSeq2]:
                lenOfSeq1 -= 1
            elif lengths[lenOfSeq1][lenOfSeq2] == lengths[lenOfSeq1][lenOfSeq2-1]:
                lenOfSeq2 -= 1
            else:
                assert seq1[lenOfSeq1-1] == seq2[lenOfSeq2-1]
                result.insert(0,seq1[lenOfSeq1-1])
                lenOfSeq1 -= 1
                lenOfSeq2 -= 1
        return result


    def SimpleLoopMatch(self, logClustL, seq):
        retLogClust = None

        for logClust in logClustL:
            if float(len(logClust.logTemplate)) < 0.5 * len(seq):
                continue

            #If the template is a subsequence of seq
            it = iter(seq)
            if all(token in seq or token == '*' for token in logClust.logTemplate):
                return logClust

        return retLogClust


    def PrefixTreeMatch(self, parentn, seq, idx):
        retLogClust = None
        length = len(seq)
        for i in xrange(idx, length):
            if seq[i] in parentn.childD:
                childn = parentn.childD[seq[i]]
                if (childn.logClust is not None):
                    constLM = [w for w in childn.logClust.logTemplate if w != '*']
                    if float(len(constLM)) >= self.tau * length:
                        return childn.logClust
                else:
                    return self.PrefixTreeMatch(childn, seq, i + 1)

        return retLogClust


    def LCSMatch(self, logClustL, seq):
        retLogClust = None

        maxLen = -1
        maxlcs = []
        maxClust = None
        set_seq = set(seq)
        size_seq = len(seq)
        for logClust in logClustL:
            set_template = set(logClust.logTemplate)
            if len(set_seq & set_template) < 0.5 * size_seq:
                continue
            lcs = self.LCS(seq, logClust.logTemplate)
            if len(lcs) > maxLen or (len(lcs) == maxLen and len(logClust.logTemplate) < len(maxClust.logTemplate)):
                maxLen = len(lcs)
                maxlcs = lcs
                maxClust = logClust

        # LCS should be large then tau * len(itself)
        if float(maxLen) >= self.tau * size_seq:
            retLogClust = maxClust

        return retLogClust


    def getTemplate(self, lcs, seq):
        retVal = []
        if not lcs:
            return retVal

        lcs = lcs[::-1]
        i = 0
        for token in seq:
            i += 1
            if token == lcs[-1]:
                retVal.append(token)
                lcs.pop()
            else:
                retVal.append('*')
            if not lcs:
                break
        if i < len(seq):
            retVal.append('*')
        return retVal

    def addSeqToPrefixTree(self, rootn, newCluster):
        parentn = rootn
        seq = newCluster.logTemplate
        seq = [w for w in seq if w != '*']

        for i in xrange(len(seq)):
            tokenInSeq = seq[i]
            # Match
            if tokenInSeq in parentn.childD:
                parentn.childD[tokenInSeq].templateNo += 1
            # Do not Match
            else:
                parentn.childD[tokenInSeq] = Node(token=tokenInSeq, templateNo=1)
            parentn = parentn.childD[tokenInSeq]

        if parentn.logClust is None:
            parentn.logClust = newCluster


    def removeSeqFromPrefixTree(self, rootn, newCluster):
        parentn = rootn
        seq = newCluster.logTemplate
        seq = [w for w in seq if w != '*']

        for tokenInSeq in seq:
            if tokenInSeq in parentn.childD:
                matchedNode = parentn.childD[tokenInSeq]
                if matchedNode.templateNo == 1:
                    del parentn.childD[tokenInSeq]
                    break
                else:
                    matchedNode.templateNo -= 1
                    parentn = matchedNode


    def outputResult(self, df_log, logClustL, logname):
        if not os.path.exists(self.savePath):
            os.makedirs(self.savePath)

        templates = [0] * df_log.shape[0]
        ids = [0] * df_log.shape[0]
        levels = [0] * df_log.shape[0]
        df_event = []

        for logclust in logClustL:
            template_str = ' '.join(logclust.logTemplate)
            level = logclust.level
            eid = hashlib.md5(template_str.encode('utf-8')).hexdigest()[0:8]
            for logid in logclust.logIDL:
                templates[logid - 1] = template_str
                ids[logid - 1] = eid
                levels[logid - 1] = level
            df_event.append([eid, level, template_str, len(logclust.logIDL)])

        df_event = pd.DataFrame(df_event, columns=['EventId', 'Level', 'EventTemplate', 'Occurrences'])
        df_event.sort_values(by=['Level'], inplace=True)
        
        df_log['EventId'] = ids
        df_log['EventTemplate'] = templates
        df_log['Level'] = levels

        df_log.to_csv(os.path.join(self.savePath, logname + '_structured.csv'), index=False)
        df_event.to_csv(os.path.join(self.savePath, logname + '_templates.csv'), index=False)


    def printTree(self, node, dep):
        pStr = ''
        for i in xrange(dep):
            pStr += '\t'

        if node.token == '':
            pStr += 'Root'
        else:
            pStr += node.token
            if node.logClust is not None:
                pStr += '-->' + ' '.join(node.logClust.logTemplate)
        print(pStr +' ('+ str(node.templateNo) + ')')

        for child in node.childD:
            self.printTree(node.childD[child], dep + 1)


    def parse(self, df_log):
        starttime = datetime.now()
        rootNode = Node()
        logCluL = []

        count = 0
        for idx, line in df_log.iterrows():
            logID = line['LineId']
            logmessageL = list(filter(lambda x: x != '', re.split(r'[\s=:,]', line['Content'])))
            constLogMessL = [w for w in logmessageL if w != '*']

            #Find an existing matched log cluster
            matchCluster = self.PrefixTreeMatch(rootNode, constLogMessL, 0)

            if matchCluster is None:
                matchCluster = self.SimpleLoopMatch(logCluL, constLogMessL)

                if matchCluster is None:
                    matchCluster = self.LCSMatch(logCluL, logmessageL)

                    # Match no existing log cluster
                    if matchCluster is None:
                        newCluster = LCSObject(logTemplate=logmessageL, logIDL=[logID], level = line['Level'])
                        logCluL.append(newCluster)
                        self.addSeqToPrefixTree(rootNode, newCluster)
                    #Add the new log message to the existing cluster
                    else:
                        newTemplate = self.getTemplate(self.LCS(logmessageL, matchCluster.logTemplate),
                                                       matchCluster.logTemplate)
                        if ' '.join(newTemplate) != ' '.join(matchCluster.logTemplate):
                            self.removeSeqFromPrefixTree(rootNode, matchCluster)
                            matchCluster.logTemplate = newTemplate
                            matchCluster.level = line['Level']
                            self.addSeqToPrefixTree(rootNode, matchCluster)
            if matchCluster:
                matchCluster.logIDL.append(logID)
            count += 1
            if count % 1000 == 0 or count == len(df_log):
                print('Processed {0:.1f}% of log lines.'.format(count * 100.0 / len(df_log)))
        self.logCluL = logCluL
        print('Parsing done. [Time taken: {!s}]'.format(datetime.now() - starttime))
