'''
complexity. more than one return because of the reactive scheme.
it is also dumb to memoize non functonal procedure
this why i will create a new methode "memoize" 
once one excution want to leave a message to the sender. it self push it in a memoise
current tracing scheme will be recycled as memoise:trace:....
    workflow is like
    hey do this function for me and create this function that return the result. i will wait it to exist by then.
it will also reduce the need of a wierd code for dynamicly generated function. these only refer to memoisedb

it is not a database

will capture any memoize:##########

every scan it will try to find all execution queue that resolved and store its result in the memoise were it expose as function with given id.

this will give usage to the huge ammount of informstion stored.

it will need lot of adjustment
but we need first to prove the concept
for now it work

it will be avantageous to use perfect dual hash as id.
because real memoize is not bound to an execution scheme maybe it is not a memoize we are dealing with
with perfect hash any look alike instruction would match and reuse part
memoise at method level can accelerate some execution
'''
def isDone(trace):
    '''
    with all info from trace. determine if done
    '''
    return trace['status'] == 'done' and not [  'at least one'
                                                for promises in trace['triggered']
                                                if not isDone(promises)             ]

def lastId(trace):
    if trace['triggered']:
        id = [lastId(promises) for promises in trace['triggered']]
    else:
        id = trace['emited']
    return id

def flatten(lst):
    if not isinstance(lst,list):
        yield lst
    for item in lst:
        if isinstance(item,list):
            for sub in flatten(item):
                yield sub
        else:
            yield item

def updateMemoiseDatabase(url,db,name,memoisedb):
    '''
    scann all mq
    and add all new finished process to memoisedb if not already there
    '''
    import couchdb,cmq
    m = couchdb.client.Database(memoisedb)
    c = cmq.connection(url,db,name)
    allId = c.report()
    for d in allId:
         id=d['id']
         if id not in m:
             t = c.trace(id)
             if isDone(t):
                 idlst = lastId(t)
                 idlst = flatten(idlst)
                 lastProcessLst = [c.fetchProcess(_id) for _id in idlst if _id]
                 resultlst = [   rpc
                                 for process in lastProcessLst
                                 if process
                                 if 'rpc' in process
                                 for rpc in process['rpc']
                                 if 'result' in rpc or 'error' in rpc    ]
                 try:
                    print("save",id)
                    m[id] = {'memoise':resultlst}
                 except:
                    pass    #ignore this is how we handle race condition

def buildAgentProcDirFromMemoise(memoisedb):
    '''
    scan the memoise db then build a processdir
    '''
    agentProcDir = {}
    import couchdb
    m = couchdb.client.Database(memoisedb)
    def getter(m,id):
        def closure(params_to_ignore):
            lastExecution = m[id]['memoise']
            #complexity, because a method trace is like tree
            #let thorw error if all of them fail
            #otherwise return list of successful result
            allResult = [ret['result'] for ret in lastExecution if 'result' in ret ]
            if not allResult:
                raise Exception(lastExecution)
            else:
                return allResult
        return closure
    for id in m:
        agentProcDir['memoise:%s'%id] = getter(m,id)
    return agentProcDir

def moreArg(parser):
    parser.add_argument('--memoise',action="store",help='will scan all database and build function')
    return parser

if __name__ == '__main__':
    import doclan
    args = doclan.doclanarg({},moreArg)
    if args.memoise:
        assert args.url
        assert args.db
        assert args.name
        updateMemoiseDatabase(args.url, args.db, args.name, args.memoise)
        agentProcDir = buildAgentProcDirFromMemoise(args.memoise)
        c = cmq.connection(args.url, args.db, args.name)
        c.process(agentProcDir)
