
import copy,sys,couchdb,json

"""
Orthogonaly Persistent Execution Queue
  -> opeq

{
    event : [
        from : ""
        emit : ""
        when : {}
        tag: "always added to document that trigger it and prevent trigger two time same document"
    ],
    src : {
        "procname" : [
            {do : fn, with: param, catch : errorproc}
            {emit: proc, with: params , from: docid}
            {let: varname, with : params, return : ret}
        ]
    }
}

ISSUE
agent announce doc increase db size DONE
$this dont work
event dont add $params
trace should be able to not infinite loop
trace should be able to list orfand process
TRACE NEED TO BE REWRITEN

REQUIREMENT


garbage collector
    named pid must stay persistent
    like archive = true meta

emit pid
   ability to control pid name
   will make execution persistant

pid ipc
  ability to name ipc when emiting
  still not sure how to do it
  readonly access for the one with pid
inter db communication
  with an agent who already connect the two db
  i think event managment should handle it

do PID parent meta
do PID stop meta that tell the object to stop
permit garbage collection of ended process
then a stop method may stop all thread from a common pid

must avoid loop. because it could lead to infinite doc creation
recursive must be impossible. this is why only emit is implemented, does not require stack because there is no return
it is not a functional paradigm, it is more monadic, forward and dont return, it is a side effect management system
trace should by done on pid doc

ABOUT
cmq, doclang proof-of-concept using couchdb
is an execution queue stored in a database. All step of execution are persistent and the computer never forget to finish a task
It is distributed, many function maybe added without code injection. every method can exist in its own environment.

all method are supported by cronjob that scan database, execute what it can and save the result. so the next cronjob can handle other dependent function.
is use JSON as simple media for document. Adopted json-rpc because it exist
added promises so all workflow are possibles with it.

the nature of the thing look alike a statemachine where all method are state and different agent handle different state. but state is discarted after process.

this document format only define execution process. not the code itself. all function need to be implemented (in python for now) in a cron job that scan the MQ.
It does not define a programe but a process with workflow. like task in gradle, rpc.document is not reusable (ok you remove the status field and it is but you know my point)
we need a way to define a procedure composed of many rpc call that chain themself as a promises.
[method:params] -> [method:params] -> [method] -> ...[method:params] or [method:params]
we need to execute it on an event (webhook, mail, form, console, rpc itself). emit is triggered by an event.
[event:payload] -> [listener:code] -> [method,payload] ... [promises] ...
ex:
let a => fn(...) or [...]   |   fn([...]).catch(let,[...]).then(save,a)

"""

##############################################################################
# test
##############################################################################
def timestamp():
    import datetime
    return datetime.datetime.utcnow()


def _crashplz(params=[]):
    raise Exception("as you wish")
def _hello(params=[]):
    line = ["hello"] + params
    return ' '.join(line)
def _write(params):
    line = ["write"] + params
    open('test.log','w').write(' '.join(line))
    return ' '.join(line)
test_directory = {
    "crashplz" : _crashplz,
    "write" : _write,
    "hello" : _hello
}

##############################################################################
# wildcard json
##############################################################################

def cmpJsonWildCard(params,wildcard):
    '''
    I had to build the spec myself
    wildcard is : '[...]' who is equal to everything
    wildcard can be inserted into text '[...]  bottles of beer on the wall, [...] bottles of beer .'
    unless wildcard, all value must equal
    dictionary, its different, params must at least contain all defined key and wildcard from value must match
    list, ist different, all element from params must match one element from wildcard
    dictionary have null, who tell you dont want that key
    list, null discarted


    >>> cmpJsonWildCard(1,1)
    True

    >>> cmpJsonWildCard('a','a')
    True

    >>> cmpJsonWildCard(True,True)
    True

    >>> cmpJsonWildCard({},{})
    True

    >>> cmpJsonWildCard([],[])
    True

    >>> cmpJsonWildCard(1,'[...]')
    True

    >>> cmpJsonWildCard('a','[...]')
    True

    >>> cmpJsonWildCard(True,'[...]')
    True

    >>> cmpJsonWildCard({},'[...]')
    True

    >>> cmpJsonWildCard([],'[...]')
    True

    >>> cmpJsonWildCard(1,'a')
    False

    >>> cmpJsonWildCard(True,'a')
    False

    >>> cmpJsonWildCard({},'a')
    False

    >>> cmpJsonWildCard([],'a')
    False

    >>> cmpJsonWildCard('a',1)
    False

    >>> cmpJsonWildCard(True,1)
    False

    >>> cmpJsonWildCard({},1)
    False

    >>> cmpJsonWildCard([],1)
    False

    >>> cmpJsonWildCard('a',True)
    False

    >>> cmpJsonWildCard(1,True)
    False

    >>> cmpJsonWildCard({},True)
    False

    >>> cmpJsonWildCard([],True)
    False

    >>> cmpJsonWildCard('a',[])
    False

    >>> cmpJsonWildCard(1,[])
    False

    >>> cmpJsonWildCard({},[])
    False

    >>> cmpJsonWildCard(True,[])
    False

    >>> cmpJsonWildCard('a',{})
    False

    >>> cmpJsonWildCard(1,{})
    False

    >>> cmpJsonWildCard(True,{})
    False

    >>> cmpJsonWildCard([],{})
    False

    >>> cmpJsonWildCard([{}],[])
    True

    >>> cmpJsonWildCard([{'a':1}],[])
    True

    >>> cmpJsonWildCard([{'a':1,'b':2}],[{'a':1}])
    True

    >>> cmpJsonWildCard([{'a':1,'b':2}],[{'b':2}])
    True

    >>> cmpJsonWildCard([{'a':1,'b':2}],[{'c':3}])
    False

    >>> cmpJsonWildCard([{'a':1},{'b':2}],[{'a':3}])
    False

    >>> cmpJsonWildCard([{'a':1,'b':2}],[{'a':'[...]'}])
    True

    >>> cmpJsonWildCard("hello","hello[...]")
    True

    >>> cmpJsonWildCard("hello world","hello[...]")
    True

    >>> cmpJsonWildCard("hello","hello")
    True

    >>> cmpJsonWildCard("hello world","hello")
    False

    >>> cmpJsonWildCard("hello","[...]world")
    False

    >>> cmpJsonWildCard("hello world","[...]world")
    True

    >>> cmpJsonWildCard("once upon a fox there was a time who ...","once upon a [...] there was a [...] who [...]")
    True

    #check for a rpc
    >>> cmpJsonWildCard({'rpc':[{'method':'nope'}]},{'rpc':[{'method':'[...]'}]})
    True

    >>> cmpJsonWildCard({'rpc':[{'meth':'nope'}]},{'rpc':[{'method':'[...]'}]})
    False

    >>> cmpJsonWildCard(['a'],[None])
    True

    >>> cmpJsonWildCard(['a'],['a',None])
    True

    >>> cmpJsonWildCard({'a':1},{'b':None})
    True

    >>> cmpJsonWildCard({'b':1},{'b':None})
    False

    >>> cmpJsonWildCard({'a':1},{'b':None,'a':'[...]'})
    True

    >>> cmpJsonWildCard({'a':1,'b':2},{'b':None,'a':'[...]'})
    False

    >>> cmpJsonWildCard({u'hey':None},{'hey':'[...]'})
    True

    '''
    if wildcard == '[...]':
        return True
    elif type(params) != type(wildcard):
        return False
    elif isinstance(params,list):
        wildcard = [ w for w in wildcard if w   ]
        #empty list only mean you want a list
        if not wildcard:
            return True
        #list with one itmes mean you want all item to comply with it
        elif len(wildcard) == 1:
            for item in params:
                if not cmpJsonWildCard(item,wildcard[0]):
                    return False
        #many items mean at least one of these must match
        else:
            return not [
                False
                for items in params
                if not [
                    True
                    for w in wildcard
                    if cmpJsonWildCard(items,w)
                ]
            ]
    #string can have wildcard within self
    elif isinstance(params,(str,unicode)):
        if not '[...]' in wildcard:
            return params == wildcard
        else:
            sublst = wildcard.split('[...]')
            tmp = params
            if len(sublst) > 0:
                sub = sublst[0]
                if not tmp.startswith(sub):
                    return False
                tmp = tmp[len(sub):]
            if len(sublst) > 1:
                for sub in sublst[1:]:
                    if sub and not sub in tmp:
                        return False
                    elif sub:
                        pos = tmp.find(sub) + len(sub)
                        tmp = tmp[pos:]
            return True
        #here wildcard string testa
    elif isinstance(params,dict):
        #recursive compare, both dict dont need to be identical
        #only that all key of wildcard is also in params
        for key in wildcard:
            if wildcard[key]:
                if not key in params:
                    return False
                elif not cmpJsonWildCard(params[key],wildcard[key]):
                    return False
            else:
                if key in params:
                    return False
        return True
    else:
        if params != wildcard:
            return False
    return True

def recursiveJsonReplace(doc,key,value):
    '''

    >>> recursiveJsonReplace({},'$key','value')
    {}

    >>> recursiveJsonReplace([],'$key','value')
    []

    >>> recursiveJsonReplace("",'$key','value')
    ''

    >>> recursiveJsonReplace({'a':1,'b':'2','c':[],'d':{}},'$key','value')
    {'a': 1, 'c': [], 'b': '2', 'd': {}}

    >>> recursiveJsonReplace(['a',2,{},[]],'$key','value')
    ['a', 2, {}, []]

    >>> recursiveJsonReplace("hello world",'$key','value')
    'hello world'

    >>> recursiveJsonReplace({'a':1,'b':'2','c':['$key'],'d':{'e':'$key'}},'$key','value')
    {'a': 1, 'c': ['value'], 'b': '2', 'd': {'e': 'value'}}

    >>> recursiveJsonReplace(['a',2,{'a':'$key'},['$key']],'$key','value')
    ['a', 2, {'a': 'value'}, ['value']]

    >>> recursiveJsonReplace("$key",'$key','value')
    'value'


    '''
    assert '$' == key[0], "value key require to start with a $"
    if isinstance(doc,dict):
        doc = dict((
            (_key,recursiveJsonReplace(_value,key,value))
            for _key,_value in doc.items()
        ))
    elif isinstance(doc,list):
        doc = [
            recursiveJsonReplace(_value,key,value)
            for _value in doc
        ]
    elif isinstance(doc,(str,unicode)):
        if doc == key:
            doc = value
    return doc

def jsonPick(doc,pick=[]):
    assert isinstance(pick,list)
    if pick:
        if isinstance(pick[0],int):
            assert isinstance(doc,lst)
            return jsonPick(doc[pick[0]],pick[1:])
        elif isinstance(pick[0],(str,unicode)):
            assert isinstance(doc,dict)
            return jsonPick(doc[pick[0]],pick[1:])
        else:
            raise Exception("invalid pick %s for document %s" % (pick,doc))
    else:
        return doc

def jsonStamp(doc,stamp):
    if type(doc) != type(stamp):
        return stamp
    elif isinstance(doc,list):
        newdoc = doc + stamp
        return newdoc
    elif isinstance(doc,dict):
        newdoc = copy.deepcopy(doc)
        for key,val in stamp.items():
            if key in newdoc:
                newdoc[key] = jsonStamp(newdoc[key],stamp[key])
            else:
                newdoc[key] = stamp[key]
        return newdoc
    else:
        return stamp

##############################################################################
# json-rpc
##############################################################################

"""
chosen json-rpc 2.0
because the reactive nature of the system make raising exception impossible
and call batch is excellent
because couchdb automagicely assign _id, 
whenever it is a callbatch or notification, it will have an _id,
that _id cant be avoided, this is why the document will be updated with result
and the document may containt context data if needed


call_example = {
    "jsonrpc" : "2.0",
    "method" : "",
    "params" : [],  #not required
    "id" : 0        #not required if no result required
}
return_example = {
    "jsonrpc" : "2.0",
    "result" : {},
    "id":0  
}
error_example = {
    "jsonrpc" : "2.0",
    "error" : {},
    "id":0
}
error_object = {
    "code" : 0,
    "message" : "",
    "data" : {}
}
error_code = {
    -32700 : "parse error, invalid json",
    -32600 : "invalid request, ",
    -32601 : "method not found, ",
    -32602 : "Invalid parameter ",
    -32603 : "Internal Error",
    -32000 : "server error"
}

using jsonrpc 2.0
the system must be able to determine that an object is already done,
we can rely on result/error update because notification dont return anything
this is why the agent will tag the document , saying, i did it

document_design = {
    '_id' : "from db",
    '_rev' : "from db",
    'rpc' : [],             #rpc call batch, return will add up once done
    'agent' : "",           #agent may sign here to tell he took care of it
    'status' : "",          #open, taken, done ... its open if not set
    'timestamp' : "",
    'log': []
}
"""


def validrpc(rpcjson):
    '''
    >>> validrpc({"method":""})
    True

    >>> validrpc({'method':"","params":[]})
    True

    >>> validrpc({'params':[]})
    False

    >>> validrpc({'method':'','then':[]})
    False

    >>> validrpc({'then':[]})
    False
    '''
    if isinstance(rpcjson,list):
        return [    True for rpc in rpcjson if validrpc(rpc)]
    elif isinstance(rpcjson,dict):
        if not 'method' in rpcjson:
            return False
        if 'then' in rpcjson and not validrpc(rpcjson['then']):
            return False
        if 'catch' in rpcjson and not validrpc(rpcjson['catch']):
            return False
        return True
    return False

def processrpc(rpcjson,procdir):
    if not 'method' in rpcjson:
        #require at least a method name
        return {
            'jsonrpc' : '2.0',
            'error' : {
                'code' : -32600,
                'message' : "The JSON sent is not a valid Request object.",
                'data' : "'method' key not found"
            }
        }
    if not 'params' in rpcjson:
        #parameter are not required, consider empty list
        rpcjson['params'] = []
    #added support for predicate instead of textual key
    #will enable regex and remote procedure
    methodname = rpcjson['method']
    methodfn = None
    for supported in procdir:
        if hasattr(supported,'__call__'):
            if supported(methodname):
                methodfn = procdir[supported]
    if not methodfn and methodname in procdir:
        methodfn = procdir[methodname]
    if not methodfn:
        #method must be un the directory
        return {
            'jsonrpc' : '2.0',
            'error' : {
                'code' : -32601,
                'message' : "The method does not exist / is not available",
                'data' : rpcjson['method']
            }
        }
    try:
        #support for dictionary instead of function as value in procdir
        if isinstance(methodfn,dict):
            methodfn = methodfn['code']
        #execute the function and return the result
        ret = methodfn(rpcjson['params'])
        return {
            'jsonrpc' : '2.0',
            'result' : ret
        }
    except:
        e = sys.exc_info()
        #the fucntion raised an exception
        return {
            'jsonrpc' : '2.0',
            'error' : {
                'code' : -32602,
                'message' : "Invalid method parameter(s). ...maybe a method error",
                'data' : str(e)
            }
        }
    #dead end
    return {
        'jsonrpc' : '2.0',
        'error' : {
            'code' : 32603,
            'message' : "Internal JSON-RPC error.",
            'data' : "code should not event go here"
        }
    }

def processcall(calljson,procdir):
    '''
    >>> processcall({'_id':'testnotification','_rev':'whatever','rpc':[{'jsonrpc':'2.0','method':'write'}]},test_directory)
    {'_rev': 'whatever', '_id': 'testnotification', 'rpc': [{'jsonrpc': '2.0', 'method': 'write'}, {'jsonrpc': '2.0', 'result': 'write'}]}

    >>> processcall({'_id':'testcall','_rev':'whatever','rpc':[{'jsonrpc':'2.0','method':'hello','id':1}]},test_directory)
    {'_rev': 'whatever', '_id': 'testcall', 'rpc': [{'jsonrpc': '2.0', 'method': 'hello', 'id': 1}, {'jsonrpc': '2.0', 'result': 'hello', 'id': 1}]}
    
    >>> processcall({'_id': 'testnotificationwparam','_rev': 'whatever','rpc': [{'jsonrpc': '2.0', 'method': 'write', 'params':['world']}]},test_directory)
    {'_rev': 'whatever', '_id': 'testnotificationwparam', 'rpc': [{'jsonrpc': '2.0', 'params': ['world'], 'method': 'write'}, {'jsonrpc': '2.0', 'result': 'write world'}]}

    >>> processcall({'_id': 'testcallwparam','_rev': 'whatever','rpc': [{'jsonrpc': '2.0', 'method': 'hello', 'params': ['world'], 'id': 1}]},test_directory)
    {'_rev': 'whatever', '_id': 'testcallwparam', 'rpc': [{'jsonrpc': '2.0', 'params': ['world'], 'method': 'hello', 'id': 1}, {'jsonrpc': '2.0', 'result': 'hello world', 'id': 1}]}
    
    >>> processcall({'_id': 'testcallbatch','_rev': 'whatever','rpc': [{'jsonrpc': '2.0', 'method': 'hello', 'id': 0},{'jsonrpc': '2.0', 'method': 'hello', 'params': ['world'], 'id': 1},{'jsonrpc': '2.0', 'method': 'write'},{'jsonrpc': '2.0', 'method': 'write', 'params': ['world']}]},test_directory)
    {'_rev': 'whatever', '_id': 'testcallbatch', 'rpc': [{'jsonrpc': '2.0', 'method': 'hello', 'id': 0}, {'jsonrpc': '2.0', 'params': ['world'], 'method': 'hello', 'id': 1}, {'jsonrpc': '2.0', 'method': 'write'}, {'jsonrpc': '2.0', 'params': ['world'], 'method': 'write'}, {'jsonrpc': '2.0', 'result': 'hello', 'id': 0}, {'jsonrpc': '2.0', 'result': 'hello world', 'id': 1}, {'jsonrpc': '2.0', 'result': 'write'}, {'jsonrpc': '2.0', 'result': 'write world'}]}

    #>>> processcall({'_id' : 'testerror','_rev' : 'whatever','rpc' : [{'jsonrpc' : '2.0', 'method': 'crashplz', 'id':0},{'jsonrpc' : '2.0', 'method': 'dontexist', 'id':1},{'jsonrpc' : '2.0', 'method': 'hello', 'params':[0], 'id':2}]},test_directory)
    {'_rev': 'whatever', '_id': 'testerror', 'rpc': [{'jsonrpc': '2.0', 'method': 'crashplz', 'id': 0}, {'jsonrpc': '2.0', 'method': 'dontexist', 'id': 1}, {'jsonrpc': '2.0', 'params': [0], 'method': 'hello', 'id': 2}, {'jsonrpc': '2.0', 'id': 0, 'error': {'message': 'Invalid method parameter(s). ...maybe a method error', 'code': -32603, 'data': "..."}}, {'jsonrpc': '2.0', 'id': 1, 'error': {'message': 'The method does not exist / is not available', 'code': -32601, 'data': "..."}}, {'jsonrpc': '2.0', 'id': 2, 'error': {'message': 'Invalid method parameter(s). ...maybe a method error', 'code': -32602, 'data': "..."}}]}
    
    >>> processcall({'rpc':{'method':'hello'}}, test_directory)
    {'rpc': [{'method': 'hello'}, {'jsonrpc': '2.0', 'result': 'hello'}]}

    >>> processcall({'rpc':{'method':'hello','then':{'method':'hello'}}}, test_directory)
    {'rpc': [{'then': {'method': 'hello'}, 'method': 'hello'}, {'then': {'method': 'hello'}, 'jsonrpc': '2.0', 'result': 'hello'}]}

    >>> processcall({'rpc':{'method':'hello','then':{'method':'hello'}, 'catch':{'method': 'hello'}}}, test_directory)
    {'rpc': [{'catch': {'method': 'hello'}, 'then': {'method': 'hello'}, 'method': 'hello'}, {'catch': {'method': 'hello'}, 'then': {'method': 'hello'}, 'jsonrpc': '2.0', 'result': 'hello'}]}

    >>> processcall({'rpc':{'method':'crashplz','then':{'method':'hello'}, 'catch':{'method': 'hello'}}}, test_directory)
    {'rpc': [{'catch': {'method': 'hello'}, 'then': {'method': 'hello'}, 'method': 'crashplz'}, {'catch': {'method': 'hello'}, 'then': {'method': 'hello'}, 'jsonrpc': '2.0', 'error': {'message': 'Invalid method parameter(s). ...maybe a method error', 'code': -32602, 'data': "..."}}]}

    >>> processcall({'rpc':{'method':'hello','then':[{'method':'hello'}], 'catch':[{'method': 'hello'}]}}, test_directory)
    {'rpc': [{'catch': [{'method': 'hello'}], 'then': [{'method': 'hello'}], 'method': 'hello'}, {'catch': [{'method': 'hello'}], 'then': [{'method': 'hello'}], 'jsonrpc': '2.0', 'result': 'hello'}]}

    >>> processcall({'rpc':{'method':'crashplz','then':[{'method':'hello'}], 'catch':[{'method': 'hello'}]}}, test_directory)
    {'rpc': [{'catch': [{'method': 'hello'}], 'then': [{'method': 'hello'}], 'method': 'crashplz'}, {'catch': [{'method': 'hello'}], 'then': [{'method': 'hello'}], 'jsonrpc': '2.0', 'error': {'message': 'Invalid method parameter(s). ...maybe a method error', 'code': -32602, 'data': "..."}}]}

    >>> processcall({'_id':'testcall','_rev':'whatever','rpc':[{'jsonrpc':'2.0','method':'hello','id':1}]},{lambda a:True: _hello})
    {'_rev': 'whatever', '_id': 'testcall', 'rpc': [{'jsonrpc': '2.0', 'method': 'hello', 'id': 1}, {'jsonrpc': '2.0', 'result': 'hello', 'id': 1}]}
 
    >>> processcall({'_id':'testcall','_rev':'whatever','rpc':[{'jsonrpc':'2.0','method':'hello','id':1}]},{lambda a:False: _hello})
    {'_rev': 'whatever', '_id': 'testcall', 'rpc': [{'jsonrpc': '2.0', 'method': 'hello', 'id': 1}, {'jsonrpc': '2.0', 'id': 1, 'error': {'message': 'The method does not exist / is not available', 'code': -32601, 'data': 'hello'}}]}
 
    '''
    if not 'rpc' in calljson:
        return None
    assert validrpc(calljson['rpc']), "must be a valide rpc"
    if isinstance(calljson['rpc'],dict):
        calljson['rpc'] = [calljson['rpc']]
    assert isinstance(calljson['rpc'],list)
    response = copy.deepcopy(calljson)
    resultjson = copy.deepcopy(calljson)
    for rpcjson in calljson['rpc']:
        response = processrpc(rpcjson,procdir)
        if 'then' in rpcjson:
            response['then'] = rpcjson['then']
        if 'catch' in rpcjson:
            response['catch'] = rpcjson['catch']
        if 'id' in rpcjson:
            response['id'] = rpcjson['id']
        resultjson['rpc'].append(response)
    return resultjson

def allmethodsupported(calljson,procdir):
    if not 'rpc' in calljson:
        print('no rpc',calljson)
        return None
    if isinstance(calljson['rpc'],dict):
        calljson['rpc'] = [calljson['rpc']]
    for callrpc in calljson['rpc']:
        assert isinstance(callrpc,dict), "must be dictionary, but got %s" % callrpc
        if not 'method' in callrpc:
            print('not valid',callrpc)
            return False
        if not callrpc['method'] in procdir:
            print('not supported',callrpc['method'],procdir.keys())
            return False
    return True

def makerpc(method,param=[],then=None,catch=None,id=None):
    '''
    make rpc object
    '''
    assert isinstance(method,(str,unicode)), "method should be str"
    assert validrpc(then) or not then, "then should be a valid rpc"
    assert validrpc(catch) or not catch, "catch should be a valid rpc"
    rpc = {
        'jsonrpc' : '2.0',
        'method' : method,
        'params' : param
    }
    if id:
        rpc['id'] = id
    if then:
        rpc['then'] = then
    if catch:
        rpc['catch'] = catch
    return rpc

def makecall(rpclst=[]):
    '''
    build callable object from rpc list
    '''
    call = {
        'rpc' : rpclst
    }
    return call

def patchParams(rpc,key,value):
    '''

    >>> patchParams([],'$key','value')
    []

    >>> patchParams({},'$key','value')
    {}

    >>> patchParams([{'params':['$key']},{'params':{'a':'$key'}}],'$key','value')
    [{'params': ['value']}, {'params': {'a': 'value'}}]

    '''
    if isinstance(rpc,list):
        rpc = [
            patchParams(doc,key,value)
            for doc in rpc
        ]
    elif isinstance(rpc,dict):
        if 'rpc' in rpc:
            rpc['rpc'] = patchParams(rpc['rpc'],key,value)
        elif 'params' in rpc:
            rpc['params'] = recursiveJsonReplace(rpc['params'],key,value)
    elif isinstance(rpc,(str,unicode)):
        rpc = recursiveJsonReplace(rpc,key,value)
    return rpc

###############################################################################
#promise implementation
###############################################################################
'''

promises like coroutine where a promises implementation can return the next
function/proc to call with or without parameters. From execution or constant.
    return Q(NextFunction,params)
    or define FirstFunction(...).then(nextfunction)
    and also give a Catch function.

flow based architecture.
o--o--o--..
    \
     --o-..

in this implementation, 
  simply returning a document with valid rpc field
  or put asised method and params , then and catch
  both may contain a list (this is why it emit new rpc)

spec:
{
    jsonrpc : "Q",  //not 2.0 anymore
    method : "",
    params : [],    //optional
    then : [more],  //optional
    catch : [more]  //optional
}
when returned value have { jsonrpc : "Q",...} , it is processed
return value is passed as parameter from call to call, unless params is defined
it is processed through the same api, keep emiting result for next run

test from
https://github.com/domenic/promise-tests/blob/master/lib/tests/always-async.js

expect : one(1)->two(1)
{
    'jsonrpc' : "Q",
    'method' : "one",
    'params' : 1,
    'then' : [
        {
           'jsonrpc' : "Q",
            'method' : "two"
        }
    ]
}

expect : one(1)->two(2)
{
    'jsonrpc' : "Q",
    'method' : "one"
    'params' : 1,
    'then' : [
        {
            'jsonrpc' : "Q",
            'method' : "two",
            'params' : 2
        }
    ]
}

expect : three(1)->two(1)->one(1)
assume three return {
    'jsonrpc' : "Q",
    'method' : "two",
    'params' : 1
}
{
    'jsonrpc' : "Q",
    'method' : "three",
    'params' : 1,
    'then' : [
        {
            'jsonrpc' : "Q",
            'method' : 'one'
        }
    ]
}

expect : crash(1)->err(1)
{
    'jsonrpc' : "Q",
    'method' : "crash",
    'params' : 1,
    'catch' : [
        {
            'jsonrpc' : "Q",
            'method' : 'err'
        }
    ]
}

expect : one(1)->crash(1)->err(1)
{
    'jsonrpc' : "Q",
    'method' : "one",
    'params' : 1,
    'then' : [
        {
            'jsonrpc' : "Q",
            'method' : 'crash',
            'catch' : [
                {
                    'jsonrpc' : "Q",
                    'method' : 'err'
                }
            ]
        }
    ]
}

expect : crash(1)->one(1)->two(1)
{
    'jsonrpc' : "Q",
    'method' : "crash",
    'params' : 1,
    'catch' : [
        {
            'jsonrpc' : "Q",
            'method' : 'one',
            'then' : [
                {
                    'jsonrpc' : "Q",
                    'method' : 'two'
                }
            ]
        }
    ]
}

'''
def fullfillpromises(fromdoc):
    '''
    TODO use meta
    TODO use $PID

    >>> fullfillpromises([{'result':{}}])
    []

    >>> fullfillpromises([{'result':{'method':'callcc'}}])
    [{'method': 'callcc'}]
    
    >>> fullfillpromises([{'result':{'method':'callcc','params':['var1']}}])
    [{'params': ['var1'], 'method': 'callcc'}]
    
    >>> fullfillpromises([{'result':{'method':'callcc'}, 'then':{'method':'callcc2'}}])
    [{'then': {'method': 'callcc2'}, 'method': 'callcc'}]
    
    >>> fullfillpromises([{'result':{'a':1}, 'then':{'method':'callcc2'}}])
    [{'params': {'a': 1}, 'method': 'callcc2'}]
    
    >>> fullfillpromises([{'result':{'a':1}, 'then':{'method':'callcc2', 'params': 2}}])
    [{'params': 2, 'method': 'callcc2'}]
    
    >>> fullfillpromises([{'error':{'msg':'test'}, 'then':{'method':'callcc2', 'params': 2}}])
    []
    
    >>> fullfillpromises([{'error':{'msg':'test'}, 'then':{'method':'callcc2', 'params': 2}, 'catch': {'method': 'callcc'}}])
    [{'params': {'msg': 'test'}, 'method': 'callcc'}]
     
    #>>> fullfillpromises([{'error':{'msg':'test'}, 'then':{'method':'callcc2', 'params': 2}, 'catch': {'method': 'callcc','params':2}}])
    [{'params': 2', method': 'callcc'}]
    
    >>> fullfillpromises([{'error':{'msg':'test'}, 'then':{'method':'callcc2', 'params': 2}, 'catch': {'method': 'callcc','params':2}}])
    [{'params': 2, 'method': 'callcc'}]
    
    >>> fullfillpromises([{'then':{'params':[],'method':'wipe'},'params':{},'method':'wipe','id':1},{'then':{'params':[],'method':'wipe'},'jsonrpc':'2.0','result':[],'id':1}])
    [{'params': [], 'method': 'wipe', 'id': 1}]
    
    >>> fullfillpromises([{'error':'','then':{'method':'nope'},'catch':{'method':'this'}}])
    [{'params': '', 'method': 'this'}]
    
    >>> fullfillpromises([{'error':'','then':[{'method':'nope','catch':[{'method': 'this'}]}]}])
    [{'params': '', 'method': 'this'}]

    >>> fullfillpromises([{'error':{'msg':'test'}, 'then':[{'method':'callcc2', 'params': 2}], 'catch': [{'method': 'callcc','params':2}]}])
    [{'params': 2, 'method': 'callcc'}]

    support $params magic word 
    >>> fullfillpromises([{'result':{'method':'callcc','params':{'test':'$params'}}}])
    [{'params': {'test': '$params'}, 'method': 'callcc'}]

    >>> fullfillpromises([{'result':{'a':1}, 'then':{'method':'callcc2','params':{'b':2,'also':'$params'}}}])
    [{'params': {'also': {'a': 1}, 'b': 2}, 'method': 'callcc2'}]

    >>> fullfillpromises([{'error':{'msg':'test'}, 'catch': {'method': 'callcc','params':['not me','$params']}}])
    [{'params': ['not me', {'msg': 'test'}], 'method': 'callcc'}]
 
'''
    def searchCatch(tmpdoc):
        #assert validrpc(tmpdoc)
        if isinstance(tmpdoc,list):
            for sub in tmpdoc:
                catch = searchCatch(sub)
                if catch:
                    return catch
        elif isinstance(tmpdoc,dict):
            if 'catch' in tmpdoc:
                return tmpdoc
            elif 'then' in tmpdoc:
                return searchCatch(tmpdoc['then'])
        return None
    #
    if isinstance(fromdoc,list):
        return [em for doc in fromdoc if doc for em in fullfillpromises(doc) if em]
    assert isinstance(fromdoc,dict), "not a dict %s"%fromdoc
    calllst = []
    if 'result' in fromdoc:
        result = fromdoc['result']
        #if result is a call, append its then and emit it
        if validrpc(result):
            if isinstance(result,dict):
                result = [result]
            assert isinstance(result,list)
            for call in result:
                call = copy.deepcopy(call)
                #unless notify, pass  on the id
                if 'id' in fromdoc:
                    call['promised'] = fromdoc['id']
                if 'then' in fromdoc:
                    call.update({'then':fromdoc['then']})
                calllst.append(call)
        #else result will be passed as params to then, unless he already has
        elif 'then' in fromdoc:
            then = fromdoc['then']
            if isinstance(then,dict):
                then = [then]
            assert isinstance(then,list)
            assert validrpc(then)
            for call in then:
                call = copy.deepcopy(call)
                if not 'params' in call:
                    call['params'] = fromdoc['result']
                else:
                    call = patchParams(call,'$params',fromdoc['result'])
                if 'id' in fromdoc:
                    call['id'] = fromdoc['id']
                calllst.append(call)
    #error mean execute the first catch you find , ascend every then if present , only if none found , return the error
    elif 'error' in fromdoc:
        error = fromdoc['error']
        tmpdoc = copy.deepcopy(fromdoc)
        tmpdoc = searchCatch(tmpdoc)
        if tmpdoc and 'catch' in tmpdoc:
            catch = tmpdoc['catch']
            if isinstance(catch,dict):
                catch = [catch]
            assert isinstance(catch,list)
            assert validrpc(catch)
            for call in catch:
                call = copy.deepcopy(call)
                if not 'params' in call:
                    call['params'] = error
                else:
                    call['params'] = patchParams(call['params'],'$params',error)
                if 'id' in fromdoc:
                    call['id'] = fromdoc['id']
                calllst.append(call)
    return calllst

###############################################################################
# metadata
###############################################################################

"""
metadata contain all mutable value that doclang may need alongside your document.
it will keep the execution status, the promise pointer, the future PID and more
{
    _id : ...,
    _rev : ...,
    meta : {},
    rpc : []
}

"""

def _metaIsStatusOpen(doc):
    '''
    will return true if meta/status != open
    '''
    wildcard = {
        'meta' : {
            'status' : "open"
        }
    }
    #return cmpJsonWildCard(doc,wildcard)
    return 'meta' in doc and 'status' in doc['meta'] and doc['meta']['status'] == 'open'

def _metaIsStatusDone(doc):
    '''
    will return true if meta/status != Done
    '''
    wildcard = {
        'meta' : {
            'status' : "done"
        }
    }
    #return cmpJsonWildCard(doc,wildcard)
    return 'meta' in doc and 'status' in doc['meta'] and doc['meta']['status'] == 'done'


def _metaIsStatusTaken(doc):
    '''
    will return true if meta/status != taken
    '''
    wildcard = {
        'meta' : {
            'status' : "taken"
        }
    }
    #return cmpJsonWildCard(doc,wildcard)
    return 'meta' in doc and 'status' in doc['meta'] and doc['meta']['status'] == 'taken'


def _metaSetStatusDone(doc):
    if not 'meta' in doc:
        doc['meta'] = {}
    doc['meta']['status'] = 'done'
    return doc

def _metaSetStatusTaken(doc):
    if not 'meta' in doc:
        doc['meta'] = {}
    doc['meta']['status'] = 'taken'
    return doc

def _metaSetStatusOpen(doc):
    if not 'meta' in doc:
        doc['meta'] = {}
    doc['meta']['status'] = 'open'
    return doc

def _metaGetStatus(doc):
    status = 'pending'
    if 'meta' in doc and 'status' in doc['meta']:
        status = doc['meta']['status']
    return status

def _metaHaveStatus(doc):
    return 'meta' in doc and 'status' in doc['meta']

def _metaIsPromised(doc):
    '''
    will return true if document promised to another
    '''
    wildcard = {
        'meta' : {
            'promised' : "[...]"
        }
    }
    #return cmpJsonWildCard(doc,wildcard)
    return 'meta' in doc and 'promised' in doc['meta']

def _metaSetPromise(doc,promise):
    if not 'meta' in doc:
        doc['meta'] = {}
    doc['meta']['promised'] = promise
    return doc

def _metaGetPromise(doc):
    promise = None
    if 'meta' in doc and 'promised' in doc['meta']:
        promise = doc['meta']['promised']
    return promise

def _metaSetEmit(doc,name):
    if not 'meta' in doc:
        doc['meta'] = {}
    doc['meta']['emit'] = name
    return doc

def _metaIsEmit(doc,name):
    return 'meta' in doc and 'emit' in doc['meta'] and doc['meta']['emit'] == name
    #return cmpJsonWildcard(doc,{
    #    'meta' : {
    #        'emit' : name
    #    }
    #})

def _metaSetProcess(doc,name):
    if not 'meta' in doc:
        doc['meta'] = {}
    doc['meta']['process'] = name
    return doc

def _metaIsProcess(doc,name):
    return 'meta' in doc and 'process' in doc['meta'] and doc['meta']['process'] == name
    #return cmpJsonWildCard(doc,{
    #    'meta' : {
    #        'process' : name
    #    }
    #})

def _metaGetProcess(doc):
    process = 'unprocessed'
    if 'meta' in doc and 'process' in doc['meta']:
        process = doc['meta']['process']
    return process

def _metaHavePid(doc):
    '''
    >>> _metaHavePid({'meta':{'pid':"yes"}})
    True

    >>> _metaHavePid({'meta':{}})
    False

    '''
    return 'meta' in doc and 'pid' in doc['meta']

def _metaGetPid(doc):
    '''
    >>> _metaGetPid({'meta':{'pid':"yes"}})
    'yes'

    >>> _metaGetPid({'meta':{}})
    Traceback (most recent call last):
        ...
    AssertionError: create it first

    '''
    assert _metaHavePid(doc), "create it first"
    if _metaHavePid(doc):
        return doc['meta']['pid']

def _metaSetPid(doc,pid):
    if not 'meta' in doc:
        doc['meta'] = {}
    doc['meta']['pid'] = pid
    return doc

def _metaList(doc):
    if 'meta' in doc:
        for key,val in doc['meta'].items():
            yield key,val

def _metaAdd(doc,key,value):
    assert not '$' in key, '$ symbol is reserved'
    if not 'meta' in doc:
        doc['meta'] = {}
    assert not key in doc['meta'], 'let assignation is immutable, %s is already set and contain %s' % (key, doc['meta'][key])
    doc['meta'][key] = value
    return doc

def _metaHaveTag(doc,tag):
    assert isinstance(doc,dict)
    if not 'meta' in doc:
        return False
    if not 'tag' in doc['meta']:
        return False
    assert isinstance(doc['meta']['tag'], list)
    return tag in doc['meta']['tag']

def _metaAddTag(doc,tag):
    assert isinstance(doc,dict)
    if not 'meta' in doc:
        doc['meta'] = {}
    if not 'tag' in doc['meta']:
        doc['meta']['tag'] = []
    assert isinstance(doc['meta']['tag'], list)
    if not tag in doc['meta']['tag']:
        doc['meta']['tag'].append(tag)
    return doc

###############################################################################
# src compile SUGAR SYNTHAX!!!
###############################################################################

'''
the sugar synthax !!!
{do : fn, with: param, catch : errorproc}
{emit: proc, with: params , from: docid}
{let: varname, with : params, return : ret}

TODO
from alone make global
from along with emit only apply on said emit
more function to come
'''

def _compileSrcToRpc(stmLst,params="$params"):
    '''
    TODO emit method

    >>> _compileSrcToRpc([
    ...   {'do' : 'first'},
    ...   {'with' : {'data':'$params'}},
    ...   {'do' : 'second', 'catch' :'hamburger'},
    ...   {'emit' : 'hotdog'}
    ... ])
    [{'params': '$params', 'jsonrpc': '2.0', 'method': 'first', 'then': [{'catch': {'params': {'do': 'hamburger', 'with': '$params', 'from': '$this'}, 'jsonrpc': '2.0', 'method': 'emit'}, 'params': '$params', 'jsonrpc': '2.0', 'method': 'second', 'then': [{'params': {'do': 'hotdog', 'with': '$params', 'from': '$this'}, 'jsonrpc': '2.0', 'method': 'emit'}]}]}]

    >>> _compileSrcToRpc([
    ...   {'let':'key1','val':'val1'},
    ...   {'let':'key2','val':'val2','ret':'$params'}
    ... ])
    []
    '''
    rpcSeq = []
    def recursive(_stmLst,_with=None,_from=None):
        #print _stmLst,_with,_from
        rpc = []
        if _stmLst:
            stm = _stmLst[0]
            stmRest = _stmLst[1:]
            _then = recursive(stmRest,_with,_from) 
            #print stm, stmRest
            if 'with' in stm:
                _with = stm['with']
            if 'from' in stm:
                _from = stm['from']
            if 'do' in stm:
                _catch = {}
                if 'catch' in stm:
                    _emit = {
                        'do':stm['catch'],
                        'with':_with,
                        'from':_from
                    }
                    _catch = makerpc('emit',_emit)
                call = makerpc(stm['do'],_with,_then,_catch)
                rpc.append(call)
            elif 'emit' in stm:
                _emit = {
                    'do':stm['emit'],
                    'with':_with,
                    'from':_from
                }
                call = makerpc('emit',_emit,_then)
                rpc.append(call)
            elif 'let' in stm:
                _emit = {
                    'key':stm['let'],
                    'val':_with
                }
                if 'ret' in stm:
                    _emit['ret'] = stm['ret']
                call = makerpc('let',_emit,_then)
                rpc.append(call)
            else:
                return _then
        return rpc
    rpc = recursive(stmLst,params,'$this')
    return rpc

###############################################################################
# connection couchdb and controler lot of code to extract
###############################################################################
class connection(object):
    def __init__(self,url,db, agent=None):
        self.server = couchdb.Server(url)
        self.db = self.server[db]
        if not agent:
            import os,binascii
            agent = binascii.b2a_hex(os.urandom(7))
        self.agent = agent

    def _allDocId(self,view):
        return [    key.id
                    for key in self.db.view(view)
                    if not '_design/' in key.id   ]

    def announce(self,procdir):
        '''
        will check if agent/name document exist then update it exist, create it othewise
        the document will compound all method supported
        '''
        _id = 'agent/%s/announce' % self.agent
        _doc = {
            '_id' : _id,
            'agent' : self.agent,
            'methodlst' : [
                {
                    'name' : methodname,
                    'about' : d['about']
                }
                for methodname,d in procdir.items()
                if isinstance(d,dict)
            ]+[
                {
                    'name' : methodname,
                    'about' : str(fn)
                }
                for methodname,fn in procdir.items()
                if hasattr(fn,'__call__')
            ]
        }
        doc = self.db.get(_id)
        if doc:
            tmp = copy.deepcopy(doc)
            del tmp['_rev']
            if tmp == _doc:
                return _id
            _doc['_rev'] = doc['_rev']
        _id,_rev = self.db.save(_doc)
        return _id

    def process(self,procdir, view='_all_docs', qty=0,stub=False,propagation=True):
        '''
        will query couchdb
            for all doc
                if status is open
                    set status taken
                    processcall
                    set status done
                    post result
        if qty is not 0, stop when [qty] doc are done
        return amount of document processed
        '''
        self.announce(procdir)
        cnt = 0
        allDoc = ( self.db.get(id) for id in self._allDocId(view) )
        allDocWithRpcField = ( doc for doc in allDoc if 'rpc' in doc )
        #all doc have meta and status, add it if required
        def addMetaStatusIfRequired(doc):
            if not _metaHaveStatus(doc):
                doc = _metaSetStatusOpen(doc)
            return doc
        allDocShouldHaveMetaStatus = ( addMetaStatusIfRequired(doc) for doc in allDocWithRpcField )
        def isOpenOrTakenBySameAgent(doc):
            #bring resilience on powerfaillure
            return _metaIsStatusOpen(doc) or (  _metaIsStatusTaken(doc) and _metaIsProcess(doc,self.agent) )
        allDocReadyToExecute = ( doc for doc in allDocShouldHaveMetaStatus if isOpenOrTakenBySameAgent(doc) )
        def encapsulateLoneRpcIntoRpcList(doc):
            if isinstance(doc['rpc'],dict):
                doc['rpc'] = [doc['rpc']]
            return doc
        allDocRpcListToExecute = ( encapsulateLoneRpcIntoRpcList(doc) for doc in allDocReadyToExecute )
        def filterOutResultOfAncientComputation(doc):
            #realy convenient when debuging
            doc['rpc'] = [    rpcjson
                          for rpcjson in doc['rpc']
                          if not 'error' in rpcjson
                          if not 'result' in rpcjson    ]
            return doc
        filteredRpcListToExecute = ( filterOutResultOfAncientComputation(doc) for doc in allDocRpcListToExecute )
        #make possible multiple different 
        #agent to share the same queue
        #if stub is set, all method in call must be supported
        RpcListWithSupportedMethod = filteredRpcListToExecute
        if not stub:
            RpcListWithSupportedMethod = ( doc for doc in filteredRpcListToExecute if allmethodsupported(doc,procdir) )

        def MarkDocRpcAsTaken(doc):
            doc = _metaSetStatusTaken(doc)
            doc = _metaSetProcess(doc,self.agent)
            return doc
        TakenDocRpcList = ( MarkDocRpcAsTaken(doc) for doc in RpcListWithSupportedMethod )
        #actual processing
        def transactSaveBeforeProcessing(doc):
            assert 'rpc' in doc, "must contain rpc %s" % doc
            assert validrpc(doc['rpc']),"must be validrpc %s" % doc
            doc = self.givePidIfNecessary(doc)
            try:
                id,rev = self.db.save(doc)
            except Exception as e:
                print e
                pass #race condition, already taken care, moving home
            return doc
        transactDocReadyToProcess = ( transactSaveBeforeProcessing(doc) for doc in TakenDocRpcList )

        #change param if $var is used
        def applyMetaVar(doc):
            #in params replace $meta  but dont crawl "then" and "catch"
            for key,val in _metaList(doc):
                doc = patchParams(doc,'$'+key,val)
            return doc
        withMetaParameterReplaced = ( applyMetaVar(doc) for doc in transactDocReadyToProcess )

        #processing here
        def doProcessAtLast(doc,procdir):
            _metaDict = doc['meta']
            def _letMethod(params):
                assert isinstance(params,dict), 'let need a dictionary'
                assert 'key' in params, 'let need key field'
                assert 'val' in params, 'let need val field'
                assert not params['key'] in _metaDict, 'let assignation are immutable, cant overide %s ' % params['key']
                #side effect, cant reassign _metaDict from closur, but can interact with the object easily.
                _metaDict[params['key']] = params['val']
                ret = params['val']
                if 'ret' in params:
                    ret = params['ret']
                return ret
            _procdir = copy.deepcopy(procdir)
            _procdir['let'] = _letMethod
            newdoc = processcall(doc,_procdir)
            _metaDict.update(newdoc['meta'])
            newdoc['meta'] = _metaDict
            newdoc['_rev'] = doc['_rev']
            return newdoc
        processedDocLst = [ doProcessAtLast(doc,procdir) for doc in withMetaParameterReplaced ]

        #promises
        def applyPromisesLogic(doc):
            #apply promise
            promises = fullfillpromises(doc['rpc'])
            if promises:
                promisedDoc = {
                    'rpc' : promises
                }
                #TODO replace $promise in params for erlyer result
                promisedDoc = _metaSetPromise(promisedDoc,doc['_id'])
                if _metaHavePid(doc):
                    promisedDoc = _metaSetPid(promisedDoc,_metaGetPid(doc))
                _metaDict = copy.deepcopy(doc['meta'])
                _metaDict.update(promisedDoc['meta'])
                promisedDoc['meta'] = _metaDict
                try:
                    pid = self.emit(promisedDoc)
                except Exception as e:
                    print (e)
            return promises
        if propagation:
            promisesLst = [ applyPromisesLogic(doc) for doc in processedDocLst ]

        for doc in processedDocLst:
            #patch let method in procdir for one with a meta patcha
            #newdoc['log'].append("%s : %s done" % (timestamp(), self.agent) )
            processedDocLst#newdow['log'] = log          #to be implemented
            _metaDict = doc['meta']
            doc = _metaSetStatusDone(doc)
            doc = _metaSetProcess(doc,self.agent)
            id,rev = self.db.save(doc)
        return 0

    def givePidIfNecessary(self,doc):
        if not _metaHavePid(doc):
            pid,_ = self.db.save({
                'pid' : {
                    'call' : doc
                },
                'meta' : {}
            })
            doc = _metaSetPid(doc,pid)
        return doc

    def emit(self,call):
        '''
        TODO emit
             must be renamed
             because this one is used by promises
             the new one generate a pid
             need compilation
        send command to couchdb
        %call is returned by makecall
        %return id of the commited data
        '''
        assert 'rpc' in call, 'Need rpc attribute'
        assert validrpc(call['rpc']), "not a valid rpc call"
        call = copy.deepcopy(call)
        call = self.givePidIfNecessary(call)
        call = _metaSetStatusOpen(call)
        call = _metaSetEmit(call,self.agent)
        call.update({
            #'log' : ["%s : %s emited"%(timestamp(),self.agent)]
        })
        id,rev = self.db.save(call)
        print('emited',id)
        return id

    def emitFromSrc(self,SrcDoc,_do,_with):
        assert isinstance(SrcDoc,dict), "expect dictionary as SrcDoc but got %s" % type(SrcDoc)
        if _do in SrcDoc['src']:
            proc = SrcDoc['src'][_do]
            call = _compileSrcToRpc(proc,_with)
            try:
               self.emit({'rpc':call})
            except Exception as e:
                raise e
        else:
            raise Exception("procedure %s not exist in %s" % (_do,_from))
        return _with

    def fetchSrc(self,_from,view='_all_docs'):
        assert isinstance(_from,(str,unicode)), "expect _from as string but got %s" % type(_from)
        try:
            SrcDoc = self.db.get(_from)
        except:
            raise Exception("document %s not exist" % _from)
        return SrcDoc


    def emitSrc(self,_from,_do,_with,view='_all_docs'):
        '''
        will find the document
        will find the procedure
        will compile to rpc
        will emit rpc
        '''
        assert isinstance(_from,(str,unicode)), "expect _from as string but got %s" % type(_from)
        assert isinstance(_do,(str,unicode)), "expect _do as string"
        assert isinstance(_with,(dict,str,unicode,int,float,list)), "expect _with to be json serialisable"
        SrcDoc = self.fetchSrc(_from,view)
        return self.emitFromSrc(SrcDoc,_do,_with)

    def report(self,view="_all_docs"):
        '''
        TODO use $PID
        search for function call and give status
        '''
        allId = self._allDocId(view)
        allDoc = (self.db.get(id) for id in allId )
        allBegin = (
                {
                  'id'      : doc['_id'],
                  'process' : _metaGetProcess(doc),
                  'trace'   : self.trace(doc['_id'],view)
                }
                for doc in allDoc
                if 'rpc' in doc
                if not _metaIsPromised(doc)
        )
        _id = "agent/%s/ps" % self.agent
        doc = self.db.get(_id)
        _doc = {
            '_id' : _id,
            "ps" : list(allBegin)
        }
        if doc:
            _doc['_rev'] = doc['_rev']
        _id, _rev = self.db.save(_doc)
        return _id

    def trace(self,id,view='_all_docs'):
        '''
        TODO use $PID
        will read the mq and find all execution trace related
        not some execution can fork
        '''
        assert self.db.get(id), "id must first exist in the database"
        #map promises and id
        allId = self._allDocId(view)
        allDoc = (self.db.get(id) for id in allId)
        allPromisedDoc = ( doc for doc in allDoc if _metaIsPromised(doc) )
        mapPromises = [
            (
                doc['_id'],
                _metaGetPromise(doc),
                _metaGetStatus(doc)
            ) for doc in allPromisedDoc ]
        #upTrace the execution until the begining
        upId = id
        while [ True 
                for emitId,_,_ in mapPromises 
                if emitId == upId]:
            upId = [    promisedId
                        for emitId,promisedId,_ in mapPromises
                        if emitId == upId            ][0]
        upDoc = self.db.get(upId)
        upStatus = _metaGetStatus(upDoc)
        #build execution tree
        def recursiveBuild(id,status):
            return {
                'emited' : id,
                'status' : status,
                'triggered' :[  recursiveBuild(emitId,status)
                                for emitId,promisedId,status in mapPromises
                                if promisedId == id]    }
        executionTree = recursiveBuild(upId,upStatus)
        return executionTree

    def garbage(self, view="_all_docs"):
        '''
        If pid field dont exist in meta then its garbage
        If have pid but pid document dont exist, garbage
        rule can be added to pid file to change behavior, to be defined
        any orphand pid are discarted

        this is minimal !!!
        TODO
        future requirement must enable to delete completed pid
        then a pid must be able to tell if there is a rpc still waiting

        ability to list all doc with pid
        check th eir status
        if all done. dispose the entire thing
        '''
        allId = list(self._allDocId(view))
        allDoc = lambda allId :( self.db.get(id) for id in allId )
        dictPidDoc = dict(( (doc['_id'],doc) for doc in allDoc(allId) if doc if 'pid' in doc ))
        allRpcDoc = lambda allId : ( doc for doc in allDoc(allId) if doc if 'rpc' in doc if not 'src' in doc )
        #remove rpc doc without pid
        allRpcWithoutPid = ( doc
                    for doc in allRpcDoc(allId)
                    if not _metaHavePid(doc) or not _metaGetPid(doc) in dictPidDoc
                    if not 'src' in doc    #dont erase if src field is present
                    )
        for doc in allRpcWithoutPid:
            print("dispose of %s" % doc['_id'])
            self.db.delete(doc)
        #remove orphan pid doc
        allActivePid = set(( _metaGetPid(doc)
                            for doc in allRpcDoc(allId)
                            if _metaHavePid(doc)   ))
        allOrphanPid = ( pdoc
                         for pid,pdoc in dictPidDoc.items()
                         if pid
                         if not pid in allActivePid )
        for pid in allOrphanPid:
            print("dispose of pid %s" % pid['_id'])
            self.db.delete(pid)
            del dictPidDoc[pid['_id']]
        #remove all done process
        allPidWithRpcStatusAllDone = (  pid
                                        for pid in dictPidDoc.keys()
                                        if not ['undone'
                                                for rpcdoc in allRpcDoc(allId)
                                                if _metaHavePid(rpcdoc)
                                                if _metaGetPid(rpcdoc) == pid
                                                if _metaGetStatus(rpcdoc) != 'done'  ]
        )
        for pid in allPidWithRpcStatusAllDone:
            print "will dispose of %s because all rpc is done" % pid
            self.db.delete(dictPidDoc[pid])
            del dictPidDoc[pid]

    def doEvent(self,view="_all_docs"):
        '''
        expect from dn
        {
            event : [
                from :
                emit :
                when :
                tag: "always added to document that trigger it and prevent trigger two time same document"
            ]
        }
        '''
        allId = list(self._allDocId(view))
        allDoc = lambda nothing :( self.db.get(id) for id in allId )
        allEventDoc = lambda nothing : ( doc for doc in allDoc(0) if 'event' in doc )
        allNonDoclang = lambda nothing : ( doc for doc in allDoc(0) if not 'event' in doc if not 'src' in doc if not 'rpc' in doc if not 'pid' in doc if not 'agent' in doc)
        allValidEvent = [   event
                            for eventDoc in allEventDoc(0)
                            for event in eventDoc['event']
                            if 'from' in event
                            if 'when' in event
                            if 'emit' in event
                            if 'tag' in event ]
        eventPerDoc = lambda doc,eventLst : [    event
                                        for event in eventLst
                                        if not _metaHaveTag(doc,event['tag'])
                                        if cmpJsonWildCard(dict(doc),event['when']) ]
        allDocWithEvent = ( (doc, eventPerDoc(doc,allValidEvent)) for doc in allNonDoclang(0) )
        allTrigger = ( (doc,event) for doc,event in allDocWithEvent  )
        for doc,eventLst in allTrigger:
            for event in eventLst:
                doc = _metaAddTag(doc,event['tag'])
                self.emitSrc(event['from'], event['emit'], doc)
            self.db.save(doc)
#############################################################################
# basic method
##############################################################################

def _make_native_procedure_available(connection):#url,dbname,agentname):
    '''
    assemble some function ready to execute
    '''
    server = connection.server
    db = connection.db
    dbname = connection.agent

    def _agent_wildcard(params):
        data = params['doc']
        wildcard = params['with']
        if not cmpJsonWildCard(data,wildcard):
            raise Exception("Dont match")
        return data

    def _agent_load(params):
        #assert isinstance(params,str), "expect document id"
        #raise Exception("not supported yet")
        try:
            doc = db.get(params)
        except Exception as e:
            doc = e
        return doc

    def _agent_save(params):
        assert isinstance(params,dict), "expect dictionary"
        #assert '_id' in params, "expect an _id"
        if '_id' in params:
            print "open"
            try:
                _doc = db.get(params['_id'])
            except:
                _doc = None
            if _doc:
                _rev = _doc['_rev']
                params['_rev'] = _rev
        try:
            _id,_rev = db.save(params)
        except Exception as e:
            _id = e
        return _id
        #raise Exception("not supported yet")

    def _agent_list(params):
        if not params:
            params = '_all_docs'
        assert isinstance(params,str), "expect view name"
        #raise Exception("not supported yet")
        try:
            listdoc = [
                key.id
                for key in db.view(params)
                if key
                if not '_design/' in key.id
            ]
        except Exception as e:
            listdoc = e
        return listdoc

    def _agent_exit(params):
        raise Exception("not supported yet")

    def _agent_fail(params):
        raise Exception(params)

    def _agent_let(params):
        return params

    def _agent_stamp(params):
        assert 'doc' in params, 'doc field required'
        assert 'with' in params, 'with field required'
        return jsonStamp(params['doc'], params['with'])

    def _agent_pick(params):
        assert 'doc' in params, 'doc field is required'
        assert 'with' in params, 'with field required'
        return jsonPick(params['doc'],params['pick'])

    def _agent_emit(params):
        '''
        will emit a rpc from src document, will be compiled btw
        params : {
            from : docid
            do : procname
            with : parameters
        }
        return : parameters
        '''
        assert 'from' in params, "require 'from' field in parameters"
        assert 'do' in params, "require 'do' field in parameters"
        if not 'with' in params:
            _with = ''
        else:
            _with = params['with']
        _do = params['do']
        _from = params['from']
        return connection.emitSrc(_from,_do,_with)
        #raise Exception("not yet implemented")

    procdir = {
        'wildcard' : {
            'code' : _agent_wildcard,
            'about' : "wildcard support [...] {a:None}"
        },
        'load' : {
            'code' : _agent_load,
            'about' : "return document at given id, expect params as string"
        },
        'save' : {
            'code' : _agent_save,
            'about' : "save document to database using id, expect params to be dict and containing _id"
        },
        'list' : {
            'code' : _agent_list,
            'about' : "return list of all document id, params can be avoided, only view required"
        },
        'fail' : {
            'code' : _agent_fail,
            'about' : "will end in a faillure and report parameters"
        },
        'let' : {
            'code' : _agent_let,
            'about' : "will just return params"
        },
        'pick' : {
            'code' : _agent_pick,
            'about' : "will find pick from doc"
        },
        'stamp' : {
            'code' : _agent_stamp,
            'about' : "will merge doc and with"
        },
        'emit' : {
            'code' : _agent_emit,
            'about' : "will emit procedure for execution with parameters"
        }

    }

    return procdir

##############################################################################
# frontend
##############################################################################

def doclanarg(procdir,addArgFn=None):
    import argparse

    parser = argparse.ArgumentParser(description='Couch Message Queue')
    parser.add_argument('--name',action='store',help='agent name')
    parser.add_argument('--db',action='store',help='database name')
    parser.add_argument('--url',action='store',default='http://localhost:5984/',help='database url')
    parser.add_argument('--process',action='store_true',help='query database and execute instruction')
    parser.add_argument('--view',action='store',default='_all_docs',help='couchdb view to use')
    parser.add_argument('--limit',action='store',default='0',help='limit quantity of instruction to run')
    parser.add_argument('--test',action='store_true',help="run unittest")
    parser.add_argument('--cycle',action='store',default=1,nargs='?',type=int,help="number of time you want to repeat this")
    if addArgFn:
        parser = addArgFn(parser)
    args = parser.parse_args()
    if args.test:
        import doctest
        doctest.testmod(optionflags=doctest.ELLIPSIS)#, verbose=True)
    if args.process:
        assert args.name
        assert args.db
        assert args.url
        assert args.view
        assert args.limit
        c = connection(args.url,args.db,args.name)
        _procdir = _make_native_procedure_available(c) #args.url,args.db,args.name)
        _procdir.update(procdir)
        for _ in range(args.cycle):
            print('processing %s%s' % (args.url, args.db))
            c.process(_procdir,view=args.view, qty=args.limit)
    return args



##############################################################################
# main
##############################################################################

if __name__ == '__main__':
    import json
    def addArgument(parser):
        parser.add_argument('--emit',action="store_true",help='will compile a method and emit it expect --do and --from')
        parser.add_argument('--with',nargs='?',help="parameters for emit, @filename or '{...}'")
        parser.add_argument('--from',nargs='?',help='src of the document where procedure can be found, @filename or <docid>', dest='from')
        parser.add_argument('--do',nargs='?',help='procedure name to emit')

        parser.add_argument('--report',action="store_true",help="return report from mq")
        parser.add_argument('--trace',action="store",help='return execution trace from mq with give id in it, even if in the middle of the trace')
        parser.add_argument('--garbage', action="store_true", help='delete all done document')
        parser.add_argument('--event', action="store_true", help='scan document and trigger event')
        return parser

    args = doclanarg({},addArgument)
    if args.emit:
        def tryJson(s):
            try:
                return json.loads(s)
            except:
                return str(s)
        def tryLoad(s):
            try:
                return open(s).read()
            except:
                return str(s)
        assert args.url
        assert args.db
        assert args.name
        c = connection(args.url,args.db,args.name)
        assert args.do, "require do"
        assert 'from' in args, "require from"
        _do = args.do
        _from = vars(args)['from']
        #get the parameters
        if not 'with' in args:
            _with = None
        else:
            _with = vars(args)['with']
        #get the source
        import re
        if re.match('^@.+',_from):
            #load src as file
            _from = tryLoad(_from)
            SrcDoc = tryJson(_from)
        else:
            #load src as doc
            SrcDoc = c.fetchSrc(_from)
        #parameter can be string or json
        if isinstance(_with,(str,unicode)) and re.match('^@.+',_with):
            #params from file
            _with = tryLoad(_with)
        _with = tryJson(_with)
        #emit the rpc
        c.emitFromSrc(SrcDoc,_do,_with)
    if args.report:
        assert args.url
        assert args.db
        assert args.name
        print('building report')
        c = connection(args.url,args.db,args.name)
        print(c.report(view=args.view))
    if args.trace:
        assert args.url
        assert args.db
        assert args.name
        print('trace execution')
        c = connection(args.url,args.db,args.name)
        print(c.trace(args.trace,view=args.view))
    if args.garbage:
        assert args.url
        assert args.db
        assert args.name
        print('trace execution')
        c = connection(args.url,args.db,args.name)
        print(c.garbage(view=args.view))
    if args.event:
        assert args.url
        assert args.db
        assert args.name
        print('trace execution')
        c = connection(args.url,args.db,args.name)
        print(c.doEvent(view=args.view))

    print('done')

##############################################################################
# eof
##############################################################################
