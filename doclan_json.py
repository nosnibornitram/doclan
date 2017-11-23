import re,copy

'''
future:
new non standar op
regex   : 
string  : 
math    : 
boolean : 

'''

def merge(doclst):
    '''
    json merge patch
    https://tools.ietf.org/html/rfc7386

    pseudo code:
    define MergePatch(Target, Patch):
      if Patch is an Object:
        if Target is not an Object:
          Target = {} # Ignore the contents and set it to an empty Object
      for each Name/Value pair in Patch:
        if Value is null:
          if Name exists in Target:
            remove the Name/Value pair from Target
        else:
          Target[Name] = MergePatch(Target[Name], Value)
          return Target
      else:
        return Patch

    >>> merge([{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"},{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":None},"tags":["example"]}])
    {'content': 'This will be unchanged', 'author': {'givenName': 'John'}, 'phoneNumber': '+01-123-456-7890', 'tags': ['example'], 'title': 'Hello!'}
    
    >>> merge([{'a': 'b'},{'a': 'c'}])
    {'a': 'c'}
    
    >>> merge([{'a': 'b'},{'b': 'c'}])
    {'a': 'b', 'b': 'c'}
    
    >>> merge([{'a': 'b'},{'a': None}])
    {}
    
    >>> merge([{'a': 'b', 'b': 'c'},{'a': None}])
    {'b': 'c'}
    
    >>> merge([{'a': ['b']},{'a': 'c'}])
    {'a': 'c'}
    
    >>> merge([{'a': 'c'},{'a': ['b']}])
    {'a': ['b']}
    
    >>> merge([{'a': {'b': 'c'}},{'a': {'c': None, 'b': 'd'}}])
    {'a': {'b': 'd'}}
    
    >>> merge([{'a': [{'b': 'c'}]},{'a': [1]}])
    {'a': [1]}
    
    >>> merge([['a', 'b'],['c', 'd']])
    ['c', 'd']
    
    >>> merge([{'a': 'b'},['c']])
    ['c']
    
    >>> merge([{'a': 'foo'},None])
    
    >>> merge([{'a': 'foo'},'bar'])
    'bar'
    
    >>> merge([{'e': None},{'a': 1}])
    {'a': 1, 'e': None}
    
    >>> merge([[1, 2],{'a': 'b', 'c': None}])
    {'a': 'b'}
    
    >>> merge([{},{'a': {'bb': {'ccc': None}}}])
    {'a': {'bb': {}}}

    '''
    if not len(doclst):
        return {}
    dest = doclst[0]
    for doc in doclst[1:]:
        if isinstance(doc,dict):
            if not isinstance(dest,dict):
                dest = {}
            for key,val in doc.items():
                if not val:
                    if key in dest:
                        del dest[key]
                else:
                    if key in dest:
                        tmp = dest[key]
                    else:
                        tmp = {}
                    dest[key] = merge([tmp,doc[key]])
        else:
            dest = doc
    return dest



'''
[~] is reserved, escape using [~0]
[/] is reserved, escape using [~1]

workflow
#1 transform first ~1 and then ~0 , avoid ~01 => ~1 => / 
    ~1 => / 
    ~0 => ~ 
#2 if the current refjson
    is object , search for member
    is array ,  search for index, or [-] mean non existent value (always throw error)


For example, given the JSON document
{
    "foo":  ["bar", "baz"],
    "":     0,
    "a/b":  1,
    "c%d":  2,
    "e^f":  3,
    "g|h":  4,
    "i\\j": 5,
    "k\"l": 6,
    " ":    7,
    "m~n":  8
}
The following JSON strings evaluate to the accompanying values:
""           // the whole document
"/foo"       ["bar", "baz"]
"/foo/0"     "bar"
"/"          0
"/a~1b"      1
"/c%d"       2
"/e^f"       3
"/g|h"       4
"/i\\j"      5
"/k\"l"      6
"/ "         7
"/m~0n"      8

FUTURE :
add set function, because json-patch will need it
'''

#test from https://tools.ietf.org/html/rfc6901
ex = {
    "foo":  ["bar", "baz"],
    "":     0,
    "a/b":  1,
    "c%d":  2,
    "e^f":  3,
    "g|h":  4,
    "i\\j": 5,
    "k\"l": 6,
    " ":    7,
    "m~n":  8
}

#test from https://github.com/janl/node-jsonpointer/blob/master/test.js
ex2 = {
  'a': 1,
  'b': {
    'c': 2
  },
  'd': {
    'e': [{ 'a': 3 }, { 'b': 4 }, { 'c': 5 }]
  }
}

_regex = re.compile(r'/([^/]*)')
_regex2 = re.compile(r'^/([^/]*)')
def ptrget(ptrjson,refjson):
    '''

    test from https://tools.ietf.org/html/rfc6901
    >>> ptrget("/foo",ex)
    ['bar', 'baz']
    >>> ptrget("/foo/0",ex)
    'bar'
    >>> ptrget("/",ex)
    0
    >>> ptrget("/a~1b",ex)
    1
    >>> ptrget("/c%d",ex)
    2
    >>> ptrget("/e^f",ex)
    3
    >>> ptrget("/g|h",ex)
    4
    >>> ptrget("/i\\j",ex)
    5
    >>> ptrget('/k\"l',ex)
    6
    >>> ptrget("/ ",ex)
    7
    >>> ptrget("/m~0n",ex)
    8

    test from https://github.com/janl/node-jsonpointer/blob/master/test.js
    >>> ptrget('/a',ex2)
    1
    >>> ptrget('/b/c',ex2)
    2
    >>> ptrget('/d/e/0/a',ex2)
    3
    >>> ptrget('/d/e/1/b',ex2)
    4
    >>> ptrget('/d/e/2/c',ex2)
    5

    >>> ptrget('a',ex2)
    Traceback (most recent call last):
      ...
    Exception: invalid json-pointer a for {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}}
    >>> ptrget('a/',ex2)
    Traceback (most recent call last):
      ...
    Exception: invalid json-pointer a/ for {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}}
    '''

    if not ptrjson.strip():
        return refjson
    #/a/b and /a/b/ is valid
    ptrjson = ptrjson.strip()
    if len(ptrjson) > 1 and ptrjson[-1] == '/':
        ptrjson = ptrjson[0:-1]
    current = copy.deepcopy(refjson)
    #for match in _regex.finditer(ptrjson):
    match = _regex2.match(ptrjson)
    if match:
        ptr = match.group(1).replace('~1','/').replace('~0','~')
        if isinstance(current,list):
            try:
                index = int(ptr)
            except:
                raise AssertionError("test op shouldn't get array element %s"%ptr)
            assert index < len(current), "Out of bounds (upper)"
            assert index > -1, "Out of bounds (lower)"
            current = current[index]
        elif isinstance(current,dict):
            key = str(ptr)
            assert key in current, "key %s not found un %s" %(key,current.keys())
            current = current[key]
        else:
            raise Exception('invalid json-ref %s for %s'%(match.group(0),current))
        ptrjson = ptrjson[len(match.group(0)):]
        if ptrjson:
            current = ptrget(ptrjson,current)
    else:
        raise Exception('invalid json-pointer %s for %s'%(ptrjson,refjson))
    return current

def ptrset(ptr,ref,value,insert=False):
    '''
    set value at ptr , return modified copy of ref
    test from https://github.com/janl/node-jsonpointer/blob/master/test.js
    but return modifed copy of document
    ex2 =>
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}}

    >>> ptrset('/a',ex2, 2)
    {'a': 2, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}}
    >>> ptrset('/b/c',ex2, 3)
    {'a': 1, 'b': {'c': 3}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}}
    >>> ptrset('/d/e/0/a',ex2, 4)
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 4}, {'b': 4}, {'c': 5}]}}
    >>> ptrset('/d/e/1/b',ex2, 5)
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 5}, {'c': 5}]}}
    >>> ptrset('/d/e/2/c',ex2, 6)
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 6}]}}

    >>> ptrset('/f/g/h/i',ex2, 6)
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}, 'f': {'g': {'h': {'i': 6}}}}
    >>> ptrset('/f/g/h/foo/-',ex2, 'test')
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}, 'f': {'g': {'h': {'foo': ['test']}}}}
    '''
    assert isinstance(ptr,(str,unicode)), "json-pointer not valid"
    if not ptr.strip():
        return value
    #/a/b is valid so should /a/b/
    ptr = ptr.strip()
    if len(ptr) > 1 and ptr[-1] == '/':
        ptr = ptr[0:-1]
    current = copy.deepcopy(ref)
    match = _regex2.match(ptr)
    if match:
        pth = match.group(1).replace('~1','/').replace('~0','~')
        nptr = ptr[len(match.group(0)):]
        if not current:
            if re.match(r'([0-9]+)|\-',pth) :
                current = []
            elif pth:
                current = {}
        if isinstance(current,list):
            if pth.strip() == '-':
                current.append(value)
            else:
                try:
                    index = int(pth)
                except:
                    raise AssertionError("Object operation on array target")
                if index < 0:
                    raise AssertionError("Out of bounds (lower)")
                if not nptr and insert:
                    current.insert(index,value)
                else:
                    assert index < len(current), "Out of bounds (upper)"
                    current[index] = ptrset(nptr,current[index],value)
        elif isinstance(current,dict):
            key = str(pth)
            sub = current[key] if key in current else None
            current[key] = ptrset(nptr,sub,value)
        elif nptr:
            raise Exception('non empty path at end')
        else:
            current = value
    else:
        raise Exception('invalid json-pointer %s for %s'%(ptrjson,refjson))
    return current

def ptrdel(ptr,ref):
    '''
    delete value at ptr , return modified copy of ref
    ex2 =>
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}}

    >>> ptrdel('/a',ex2)
    {'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}}
    >>> ptrdel('/b/c',ex2)
    {'a': 1, 'b': {}, 'd': {'e': [{'a': 3}, {'b': 4}, {'c': 5}]}}
    >>> ptrdel('/d/e/0/a',ex2)
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{}, {'b': 4}, {'c': 5}]}}
    >>> ptrdel('/d/e/1/b',ex2)
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {}, {'c': 5}]}}
    >>> ptrdel('/d/e/2/c',ex2)
    {'a': 1, 'b': {'c': 2}, 'd': {'e': [{'a': 3}, {'b': 4}, {}]}}
    
    >>> ptrdel('/f/g/h/i',ex2)
    Traceback (most recent call last):
      ...
    AssertionError: f not in ['a', 'b', 'd']
    
    >>> ptrdel('/f/g/h/foo/-',ex2)
    Traceback (most recent call last):
      ...
    AssertionError: f not in ['a', 'b', 'd']
    '''
    assert isinstance(ptr,(str,unicode)), "json-pointer invalid"
    if not ptr.strip():
        return ref
    #/a/b is valid so should /a/b/
    ptr = ptr.strip()
    if len(ptr) > 1 and ptr[-1] == '/':
        ptr = ptr[0:-1]
    current = copy.deepcopy(ref)
    match = _regex2.match(ptr)
    if match:
        pth = match.group(1).replace('~1','/').replace('~0','~')
        nptr = ptr[len(match.group(0)):]
        if isinstance(current,list):
            try:
                index = int(pth)
            except:
                raise AssertionError("Shouldn't remove from array with bad number")
            assert index > -1 and index < len(current), "shouldn't remove from array with bad number"
            if nptr:
                current[index] = ptrdel(nptr,current[index])
            else:
                current.pop(index)
        elif isinstance(current,dict):
            key = str(pth).strip()
            assert key in current, '%s not in %s' %(key,current.keys())
            if nptr:
                current[key] = ptrdel(nptr,current[key])
            else:
                del current[key]
        elif nptr:
            raise Exception("non existant pointer %s"%(nptr))
            #not allowed but possible
        else:
            raise Exception("problem")
            #should not be possible
    else:
        raise Exception('invalid json-pointer %s for %s'%(ptrjson,refjson))
    return current

def _mock(dic={}):
    def closure(key):
        return dic[key]
    return closure
def reference(fulldoc, fetch=lambda a: a):
    '''
    json-reference 
    http://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03

    test from https://github.com/johnnoone/json-spec/blob/master/tests/test_reference.py
    >>> reference({'foo': ['bar', 'baz']})
    {'foo': ['bar', 'baz']}

    >>> reference({'foo': ['bar', 'baz', {'$ref': '#/sub'}],'sub': 'quux'})
    {'foo': ['bar', 'baz', 'quux'], 'sub': 'quux'}

    >>> reference({'foo': ['bar', 'baz', {'$ref': 'obj2#/sub'}]}, fetch=_mock({'obj2': {'sub': 'quux'}}))
    {'foo': ['bar', 'baz', 'quux']}

    >>> reference('obj2#/sub/0', fetch=_mock({'obj1':{'foo': ['bar', 'baz', {'$ref': 'obj2#/sub/0'}]},'obj2':{'sub': [{'$ref': '#/sub2'}],'sub2': 'quux'}}))
    'quux'
    '''
    def parseRefAndFetch(ref):
        assert isinstance(ref,(str,unicode)), "json-reference invalid %s" % ref
        match = re.match(r'([^#]+)?(#([^#]+))?',ref)
        assert match,"this is not a valide json reference %s"%ref
        url = match.group(1)
        path = match.group(3)
        if url:
            rdoc = fetch(url)
            #infinite recursive ?
            #rdoc = reference(rdoc,fetch)
        else:
            rdoc = fulldoc
        if path:
            ndoc = ptrget(path,rdoc)
        else:
            ndoc = rdoc
        return ndoc
    def recursive(doc):
        if isinstance(doc,str):
	    try:
	        ref = doc
	        ndoc = parseRefAndFetch(ref)
	    except:
	        ndoc = doc
        elif isinstance(doc,dict):
            if '$ref' in doc:
	        ref = doc['$ref']
	        assert isinstance(ref,(str,unicode)), "json-reference invalid s%" % ref
	        ndoc = parseRefAndFetch(ref)
            else:
	        ndoc = dict([
	            (key,recursive(item))
		    for key,item in doc.items()
                ])
        elif isinstance(doc,list):
	    ndoc = [
	        recursive(item)
                for item in doc
            ]
        else:
            ndoc = copy.deepcopy(doc)
        return ndoc
    return recursive(fulldoc)

'''
foreach operation in
[
     { "op": "test", "path": "/a/b/c", "value": "foo" },
     { "op": "remove", "path": "/a/b/c" },
     { "op": "add", "path": "/a/b/c", "value": [ "foo", "bar" ] },
     { "op": "replace", "path": "/a/b/c", "value": 42 },
     { "op": "move", "from": "/a/b/c", "path": "/a/b/d" },
     { "op": "copy", "from": "/a/b/d", "path": "/a/b/e" }
]
execute with the return of the returning last function
until all done or error condition encountered

Operation object
{
    op : "operationname", #required , one of [add,remove,replace,move,copu,test]
    path : "",            #required , json-pointer RFC6901, the target location
    other attribute depend on [op] value
}

{
    op : "add",     #if array, concat
    path : "",      #if object, create/replace
    value : {}
}

{
    op : "remove",  #remove
    path : ""
}

{
    op : "replace",
    path : "",
    value : ""
}

{
    op : "move",
    from : "",
    path : ""
}
spec
http://jsonapi.org/extensions/jsonpatch/
https://tools.ietf.org/html/rfc6902
http://jsonpatch.com/

test case
https://github.com/json-patch/json-patch-tests
'''

def patch_add(doc,patch):
    '''
    https://tools.ietf.org/html/rfc6902#section-4.1
    The "add" operation performs one of the following functions
        - The "add" operation performs one of the following functions
        - The "add" operation performs one of the following functions
        - The "add" operation performs one of the following functions
    The "add" operation performs one of the following functions
    '''
    assert 'path' in patch, "missing 'path' parameter"
    assert 'value' in patch, "missing 'value' parameter"
    path = patch['path']
    value = patch['value']
    doc = ptrset(path,doc,value,True)
    return doc

def patch_remove(doc,patch):
    '''
    https://tools.ietf.org/html/rfc6902#section-4.2
    The "remove" operation removes the value at the target location.
    The target location MUST exist for the operation to be successful.
    If removing an element from an array, any elements above the specified index are shifted one position to the left.
    '''
    assert 'path' in patch, "missing 'path' parameter"
    path = patch['path']
    doc = ptrdel(path,doc)
    return doc

def patch_replace(doc,patch):
    '''
    https://tools.ietf.org/html/rfc6902#section-4.3
    The "replace" operation replaces the value at the target location with a new value.  The operation object MUST contain a "value" member whose content specifies the replacement value.
    The target location MUST exist for the operation to be successful.
This operation is functionally identical to a "remove" operation for a value, followed immediately by an "add" operation at the same location with the replacement value.
    '''
    assert 'path' in patch, "missing 'path' parameter"
    assert 'value' in patch, "missing 'value' parameter"
    path = patch['path']
    value = patch['value']
    old = ptrget(path,doc)
    doc = ptrset(path,doc,value,False)
    return doc

def patch_move(doc,patch):
    '''
    https://tools.ietf.org/html/rfc6902#section-4.4
    The "move" operation removes the value at a specified location and adds it to the target location.
    The operation object MUST contain a "from" member, which is a string containing a JSON Pointer value that references the location in the target document to move the value from.
    The "from" location MUST exist for the operation to be successful.
    This operation is functionally identical to a "remove" operation on the "from" location, followed immediately by an "add" operation at the target location with the value that was just removed.
    The "from" location MUST NOT be a proper prefix of the "path" location; i.e., a location cannot be moved into one of its children.
    '''
    assert 'from' in patch, "missing 'from' parameter"
    assert 'path' in patch, "missing 'path' parameter"
    path = patch['path']
    fro_ = patch['from']
    value = ptrget(fro_,doc)
    doc = ptrdel(fro_,doc)
    doc = ptrset(path,doc,value)
    return doc

def patch_copy(doc,patch):
    '''
    https://tools.ietf.org/html/rfc6902#section-4.5
    The "copy" operation copies the value at a specified location to the target location.
    The operation object MUST contain a "from" member, which is a string containing a JSON Pointer value that references the location in the target document to copy the value from.
    The "from" location MUST exist for the operation to be successful.
    This operation is functionally identical to an "add" operation at the target location using the value specified in the "from" member.
    '''
    assert 'from' in patch, "missing 'from' parameter"
    assert 'path' in patch, "missing 'path' parameter"
    path = patch['path']
    fro_ = patch['from']
    value = ptrget(fro_,doc)
    doc = ptrset(path,doc,value)
    return doc

def patch_test(doc,patch):
    '''
    https://tools.ietf.org/html/rfc6902#section-4.6
    The "test" operation tests that a value at the target location is equal to a specified value.
    The operation object MUST contain a "value" member that conveys the value to be compared to the target location's value.
    The target location MUST be equal to the "value" value for the operation to be considered successful.
    Here, "equal" means that the value at the target location and the value conveyed by "value" are of the same JSON type, and that they are considered equal by the following rules for that type:
    - strings: are considered equal if they contain the same number of Unicode characters and their code points are byte-by-byte equal.
    - numbers: are considered equal if their values are numerically equal.
    - arrays: are considered equal if they contain the same number of values, and if each value can be considered equal to the value at the corresponding position in the other array, using this list of type-specific rules.
    - objects: are considered equal if they contain the same number of members, and if each member can be considered equal to a member in the other object, by comparing their keys (as strings) and their values (using this list of type-specific rules).
    - literals (false, true, and null): are considered equal if they are the same.
    Note that the comparison that is done is a logical comparison; e.g., whitespace between the member values of an array is not significant.
    Also, note that ordering of the serialization of object members is not significant.
    '''
    assert 'path' in patch, "missing 'path' parameter"
    assert 'value' in patch, "missing 'value' parameter"
    path = patch['path']
    value = patch['value']
    #raise AssertionError("test op shouldn't get array element 1")
    if ptrget(path,doc) == value:
        return doc
    return None

patch_op = {
    'add' : patch_add,
    'remove' : patch_remove,
    'replace' : patch_replace,
    'move' : patch_move,
    'copy' : patch_copy,
    'test' : patch_test
}

def patch(doc,ptch,opd=patch_op):
    '''
    @patch_op can be extended, ex json-predicate json-math

    test from https://github.com/json-patch/json-patch-tests/blob/master/tests.json

    >>> patch({},[])
    {}
    
    >>> patch({'foo': 1},[])
    {'foo': 1}
    
    >>> patch({'foo': 1, 'bar': 2},[])
    {'foo': 1, 'bar': 2}
    
    >>> patch([{'foo': 1, 'bar': 2}],[])
    [{'foo': 1, 'bar': 2}]
    
    >>> patch({'foo': {'foo': 1, 'bar': 2}},[])
    {'foo': {'foo': 1, 'bar': 2}}
    
    >>> patch({'foo': None},[{'path': '/foo', 'value': 1, 'op': 'add'}])
    {'foo': 1}
    
    >>> patch([],[{'path': '/0', 'value': 'foo', 'op': 'add'}])
    ['foo']
    
    >>> patch(['foo'],[])
    ['foo']
    
    >>> patch({},[{'path': '/foo', 'value': '1', 'op': 'add'}])
    {'foo': '1'}
    
    >>> patch({},[{'path': '/foo', 'value': 1, 'op': 'add'}])
    {'foo': 1}
    
    >>> patch('foo',[{'path': '', 'value': 'bar', 'op': 'replace'}])
    'bar'
    
    >>> patch({},[{'path': '/', 'value': 1, 'op': 'add'}])
    {'': 1}
    
    >>> patch({'foo': 1},[{'path': '/bar', 'value': [1, 2], 'op': 'add'}])
    {'foo': 1, 'bar': [1, 2]}
    
    >>> patch({'foo': 1, 'baz': [{'qux': 'hello'}]},[{'path': '/baz/0/foo', 'value': 'world', 'op': 'add'}])
    {'foo': 1, 'baz': [{'qux': 'hello', 'foo': 'world'}]}
    
    >>> patch({'foo': 1},[{'path': '/bar', 'value': True, 'op': 'add'}])
    {'foo': 1, 'bar': True}
    
    >>> patch({'foo': 1},[{'path': '/bar', 'value': False, 'op': 'add'}])
    {'foo': 1, 'bar': False}
    
    >>> patch({'foo': 1},[{'path': '/bar', 'value': None, 'op': 'add'}])
    {'foo': 1, 'bar': None}
    
    >>> patch({'foo': 1},[{'path': '/0', 'value': 'bar', 'op': 'add'}])
    {'0': 'bar', 'foo': 1}
    
    >>> patch(['foo'],[{'path': '/1', 'value': 'bar', 'op': 'add'}])
    ['foo', 'bar']
    
    >>> patch(['foo', 'sil'],[{'path': '/1', 'value': 'bar', 'op': 'add'}])
    ['foo', 'bar', 'sil']
    
    >>> patch(['foo', 'sil'],[{'path': '/0', 'value': 'bar', 'op': 'add'}])
    ['bar', 'foo', 'sil']
    
    >>> patch(['foo', 'sil'],[{'path': '/2', 'value': 'bar', 'op': 'add'}])
    ['foo', 'sil', 'bar']
    
    >>> patch({'1e0': 'foo'},[{'path': '/1e0', 'value': 'foo', 'op': 'test'}])
    {'1e0': 'foo'}
    
    >>> patch(['foo', 'sil'],[{'path': '/1', 'value': ['bar', 'baz'], 'op': 'add'}])
    ['foo', ['bar', 'baz'], 'sil']
    
    >>> patch({'foo': 1, 'bar': [1, 2, 3, 4]},[{'path': '/bar', 'op': 'remove'}])
    {'foo': 1}
    
    >>> patch({'foo': 1, 'baz': [{'qux': 'hello'}]},[{'path': '/baz/0/qux', 'op': 'remove'}])
    {'foo': 1, 'baz': [{}]}
    
    >>> patch({'foo': 1, 'baz': [{'qux': 'hello'}]},[{'path': '/foo', 'value': [1, 2, 3, 4], 'op': 'replace'}])
    {'foo': [1, 2, 3, 4], 'baz': [{'qux': 'hello'}]}
    
    >>> patch({'foo': [1, 2, 3, 4], 'baz': [{'qux':'hello'}]},[{'path':'/baz/0/qux','value':'world','op':'replace'}])
    {'foo': [1, 2, 3, 4], 'baz': [{'qux': 'world'}]}
    
    >>> patch(['foo'],[{'path': '/0', 'value': 'bar', 'op': 'replace'}])
    ['bar']
    
    >>> patch([''],[{'path': '/0', 'value': 0, 'op': 'replace'}])
    [0]
    
    >>> patch([''],[{'path': '/0', 'value': True, 'op': 'replace'}])
    [True]
    
    >>> patch([''],[{'path': '/0', 'value': False, 'op': 'replace'}])
    [False]
    
    >>> patch([''],[{'path': '/0', 'value': None, 'op': 'replace'}])
    [None]
    
    >>> patch(['foo', 'sil'],[{'path': '/1', 'value': ['bar', 'baz'], 'op': 'replace'}])
    ['foo', ['bar', 'baz']]
    
    >>> patch({'foo': 'bar'},[{'path': '', 'value': {'baz': 'qux'}, 'op': 'replace'}])
    {'baz': 'qux'}
    
    >>> patch({'foo': 1},[{'spurious': 1, 'path': '/foo', 'value': 1, 'op': 'test'}])
    {'foo': 1}
    
    >>> patch({'foo': None},[{'path': '/foo', 'value': None, 'op': 'test'}])
    {'foo': None}
    
    >>> patch({'foo': None},[{'path': '/foo', 'value': 'truthy', 'op': 'replace'}])
    {'foo': 'truthy'}
    
    >>> patch({'foo': None},[{'path': '/bar', 'from': '/foo', 'op': 'move'}])
    {'bar': None}
    
    >>> patch({'foo': None},[{'path': '/bar', 'from': '/foo', 'op': 'copy'}])
    {'foo': None, 'bar': None}
    
    >>> patch({'foo': None},[{'path': '/foo', 'op': 'remove'}])
    {}
    
    >>> patch({'foo': 'bar'},[{'path': '/foo', 'value': None, 'op': 'replace'}])
    {'foo': None}
    
    >>> patch({'foo': {'foo': 1, 'bar': 2}},[{'path': '/foo', 'value': {'foo': 1, 'bar': 2}, 'op': 'test'}])
    {'foo': {'foo': 1, 'bar': 2}}
    
    >>> patch({'foo': [{'foo': 1, 'bar': 2}]},[{'path': '/foo', 'value': [{'foo': 1, 'bar': 2}], 'op': 'test'}])
    {'foo': [{'foo': 1, 'bar': 2}]}
    
    >>> patch({'foo': {'bar': [1, 2, 5, 4]}},[{'path': '/foo', 'value': {'bar': [1, 2, 5, 4]}, 'op': 'test'}])
    {'foo': {'bar': [1, 2, 5, 4]}}
    
    >>> patch({'foo': {'bar': [1, 2, 5, 4]}},[{'path': '/foo', 'value': [1, 2], 'op': 'test'}])
    
    >>> patch({'foo': 1},[{'path': '', 'value': {'foo': 1}, 'op': 'test'}])
    {'foo': 1}
    
    >>> patch({'': 1},[{'path': '/', 'value': 1, 'op': 'test'}])
    {'': 1}
    
    >>> doc={'': 0, 'g|h': 4, 'c%d': 2, 'a/b': 1, 'e^f': 3, ' ': 7, 'k"l': 6, 'foo': ['bar', 'baz'], 'i\\j': 5, 'm~n': 8}
    >>> patch({
    ...     '': 0,
    ...     'g|h': 4,
    ...     'c%d': 2,
    ...     'a/b': 1,
    ...     'e^f': 3,
    ...     ' ': 7,
    ...     'k"l': 6,
    ...     'foo': ['bar', 'baz'],
    ...     'i\\j': 5,
    ...     'm~n': 8
    ... },[
    ...     {'path': '/foo', 'value': ['bar', 'baz'], 'op': 'test'},
    ...     {'path': '/foo/0', 'value': 'bar', 'op': 'test'},
    ...     {'path': '/', 'value': 0, 'op': 'test'},
    ...     {'path': '/a~1b', 'value': 1, 'op': 'test'},
    ...     {'path': '/c%d', 'value': 2, 'op': 'test'},
    ...     {'path': '/e^f', 'value': 3, 'op': 'test'},
    ...     {'path': '/g|h', 'value': 4, 'op': 'test'},
    ...     {'path': '/i\\j', 'value': 5, 'op': 'test'},
    ...     {'path': '/k"l', 'value': 6, 'op': 'test'},
    ...     {'path': '/ ', 'value': 7, 'op': 'test'},
    ...     {'path': '/m~0n', 'value': 8, 'op': 'test'}
    ... ])
    {'': 0, 'g|h': 4, 'c%d': 2, 'a/b': 1, 'e^f': 3, ' ': 7, 'k"l': 6, 'foo': ['bar', 'baz'], 'i\\\\j': 5, 'm~n': 8}
    
    >>> patch({'foo': 1},[{'path': '/foo', 'from': '/foo', 'op': 'move'}])
    {'foo': 1}
    
    >>> patch({'foo': 1, 'baz': [{'qux': 'hello'}]},[{'path': '/bar', 'from': '/foo', 'op': 'move'}])
    {'bar': 1, 'baz': [{'qux': 'hello'}]}
    
    >>> patch({'bar': 1, 'baz': [{'qux': 'hello'}]},[{'path': '/baz/-', 'from': '/baz/0/qux', 'op': 'move'}])
    {'baz': [{}, 'hello'], 'bar': 1}
    
    >>> patch({'bar': 1, 'baz': [{'qux': 'hello'}]},[{'path': '/boo', 'from': '/baz/0', 'op': 'copy'}])
    {'bar': 1, 'baz': [{'qux': 'hello'}], 'boo': {'qux': 'hello'}}
    
    >>> patch({'foo': 'bar'},[{'path': '', 'value': {'baz': 'qux'}, 'op': 'add'}])
    {'baz': 'qux'}
    
    >>> patch([1, 2],[{'path': '/-', 'value': {'foo': ['bar', 'baz']}, 'op': 'add'}])
    [1, 2, {'foo': ['bar', 'baz']}]
    
    >>> patch([1, 2, [3, [4, 5]]],[{'path': '/2/1/-', 'value': {'foo': ['bar', 'baz']}, 'op': 'add'}])
    [1, 2, [3, [4, 5, {'foo': ['bar', 'baz']}]]]
    
    >>> patch([1, 2, 3, 4],[{'path': '/0', 'op': 'remove'}])
    [2, 3, 4]
    
    >>> patch([1, 2, 3, 4],[{'path': '/1', 'op': 'remove'}, {'path': '/2', 'op': 'remove'}])
    [1, 3]
    
    >>> patch([1, 2, 3, 4],[{'path': '/1e0', 'op': 'remove'}])
    Traceback (most recent call last):
        ...
    AssertionError: Shouldn't remove from array with bad number
    
    >>> patch([''],[{'path': '/1e0', 'value': False, 'op': 'replace'}])
    Traceback (most recent call last):
        ...
    AssertionError: test op shouldn't get array element 1e0
    
    >>> patch({'bar': 1, 'baz': [1, 2, 3]},[{'path': '/boo', 'from': '/baz/1e0', 'op': 'copy'}])
    Traceback (most recent call last):
        ...
    AssertionError: test op shouldn't get array element 1e0
    
    >>> patch({'foo': 1, 'baz': [1, 2, 3, 4]},[{'path': '/foo', 'from': '/baz/1e0', 'op': 'move'}])
    Traceback (most recent call last):
        ...
    AssertionError: test op shouldn't get array element 1e0
    
    >>> patch(['foo', 'sil'],[{'path': '/1e0', 'value': 'bar', 'op': 'add'}])
    Traceback (most recent call last):
        ...
    AssertionError: Object operation on array target
    
    >>> patch([1],[{'path': '/-', 'op': 'add'}])
    Traceback (most recent call last):
        ...
    AssertionError: missing 'value' parameter
    
    >>> patch([1],[{'path': '/0', 'op': 'replace'}])
    Traceback (most recent call last):
        ...
    AssertionError: missing 'value' parameter
    
    >>> patch([None],[{'path': '/0', 'op': 'test'}])
    Traceback (most recent call last):
        ...
    AssertionError: missing 'value' parameter
    
    >>> patch([False],[{'path': '/0', 'op': 'test'}])
    Traceback (most recent call last):
        ...
    AssertionError: missing 'value' parameter
    
    >>> patch([1],[{'path': '/-', 'op': 'copy'}])
    Traceback (most recent call last):
        ...
    AssertionError: missing 'from' parameter
    
    >>> patch({'foo': 1},[{'path': '', 'op': 'move'}])
    Traceback (most recent call last):
        ...
    AssertionError: missing 'from' parameter
    
    >>> patch({'foo':'bar'},[{'path': '/baz', 'from': '/foo', 'value': 'qux', 'op': 'move'}])
    {'baz': 'bar'}
    
    >>> patch({'foo':1},[{'path': '/foo', 'value': 1, 'op': 'spam'}])
    Traceback (most recent call last):
      ...
    AssertionError: Unrecognized op 'spam'
    
    >>> patch({'bar': [1, 2]},[{'path': '/bar/8', 'value': '5', 'op': 'replace'}])
    Traceback (most recent call last):
      ...
    AssertionError: Out of bounds (upper)
    
    >>> patch({'bar': [1, 2]},[{'path': '/bar/-1', 'value': '5', 'op': 'replace'}])
    Traceback (most recent call last):
      ...
    AssertionError: Out of bounds (lower)
    
    >>> patch(['foo', 'sil'],[{'path': '/3', 'value': 'bar', 'op': 'replace'}])
    Traceback (most recent call last):
      ...
    AssertionError: Out of bounds (upper)
    
    >>> patch(['foo', 'bar'],[{'path': '/1e0', 'value': 'bar', 'op': 'test'}])
    Traceback (most recent call last):
      ...
    AssertionError: test op shouldn't get array element 1e0
    
    >>> patch(['foo', 'sil'],[{'path': '/bar', 'value': 42, 'op': 'add'}])
    Traceback (most recent call last):
      ...
    AssertionError: Object operation on array target
    
    >>> patch({'foo': 1, 'baz': [{'qux': 'hello'}]},[{'path': '/baz/1e0/qux', 'op': 'remove'}])
    Traceback (most recent call last):
      ...
    AssertionError: Shouldn't remove from array with bad number
    
    '''
    if isinstance(ptch,list):
        result = doc
        for op in ptch:
            result = patch(result,op,opd)
        return result
    elif isinstance(ptch,dict):
        assert 'op' in ptch, "Missing 'op' parameter"
        assert ptch['op'] in opd,"Unrecognized op '%s'"%ptch['op']
        op = ptch['op']
        fn = opd[op]
        result = fn(doc,ptch)
        return result
    return doc

'''
json-predicate
http://tools.ietf.org/id/draft-snell-json-test-01.html

'''
predicate_ex = {
    "num1":1,
    "null1":None,
    "stringA":"A",
    "stringABC":"ABC",
    "stringAbC_123":"AbC_123",
    "stringXYZ":"XYZ",
    "stringMNO":"MNO",
    "arrayA":["a","b","c"],
    "objA":{
        "num2":2,
        "null2":None,
        "boolT":True,
        "boolF":False,
        "dateObj":"2010-10-10T10:10:10.000Z",
        "dateTime":"2010-10-10T10:10:10Z",
        "dateTimeOffset":"2010-10-10T10:10:10+05:30",
        "date":"2010-10-10",
        "timeZ":"10:10:10Z",
        "timeOffset":"10:10:10+05:30",
        "lang":"en-US",
        "langRange":"CH-*",
        "langRange2":"*",
        "langRange3":"CH-de",
        "iri":"https://github.com/MalcolmDwyer/json-predicate#test",
        "absoluteIri":"https://github.com/MalcolmDwyer/json-predicate",
        "stringX":"X",
        "stringXYZ":"XYZ",
        "stringXyZ_789":"XyZ_789",
        "objB":{
            "num3":3,
            "null3":None,
            "stringM":
            "M","stringMNO":
            "MNO","stringMnO_456":
            "MnO_456"
        }
    },
    "objX":{
        "num1":1,
        "stringAbc":"Abc",
        "objY":{"num2":2}
    }
}

def pred_contains(doc,pred):
    assert isinstance(pred,dict), "predicate must be a dict"
    assert 'path' in pred, "no path"
    assert 'value' in pred, "no value"
    value = pred['value']
    assert isinstance(value,(str,unicode)), "expected value as string"
    path = pred['path']
    fetched = ptrget(path,doc)
    assert isinstance(fetched,(str,unicode)), "expected string at %s" % path
    if 'ignore_case' in pred and pred['ignore_case']:
        value = value.upper()
        fetched = fetched.upper()
    assert value in fetched, "value is not present in %s" % path
    return doc

import re
def pred_matches(doc,pred):
    assert isinstance(pred,dict), "predicate should be a obj"
    assert 'path' in pred, "no path"
    assert 'value' in pred, "no value"
    path = pred['path']
    regex = pred['value']
    fetched = ptrget(path,doc)
    assert isinstance(fetched,(str,unicode)), "expect string at %s" % path
    if 'ignore_case' in pred and pred['ignore_case']:
        assert re.match(regex,fetched,re.I), "%s dont match %s" % (regex,fetched) 
    else:
        assert re.match(regex,fetched), "%s dont match %s" % (regex,fetched)
    return doc

def pred_in(doc,pred):
    assert isinstance(pred,dict), "predicate should be an obj"
    assert 'path' in pred, "no path"
    assert 'value' in pred, "no value"
    path = pred['path']
    value = pred['value']
    if isinstance(value,dict):
        value = list(value.values())
    assert isinstance(value,list), "expected value to be a list"
    fetched = ptrget(path,doc)
    if 'ignore_case' in pred:
        ignore_case = pred['ignore_case']
    else:
        ignore_case = False
    assert [True for val in value if _cmp(val,fetched,ignore_case)], "not in"
    return doc

def _cmp(data,moredata,ignore_case=False):
    '''
    >>> _cmp(1,1)
    True

    >>> _cmp('a','a')
    True

    >>> _cmp(True,True)
    True

    >>> _cmp(1,True)
    False

    >>> _cmp('a',1)
    False

    >>> _cmp([1,2,3],[1,2,3])
    True

    >>> _cmp([3,2,1],[3,2,1])
    True

    >>> _cmp([1,2],[2,3])
    False

    >>> _cmp([1,2],[1,2,3])
    False

    >>> _cmp([1,2,3],[2,3])
    False

    >>> _cmp({},{})
    True

    >>> _cmp({'a':1},{'a':1})
    True

    >>> _cmp({'a':{'b':{'c':[1,2,3]}}},{'a':{'b':{'c':[1,2,3]}}})
    True

    >>> _cmp({'a':{'b':{'c':3}}},{'a':{'b':{'c':3}}})
    True

    >>> _cmp({'a':{'b':3}},{'a':{'b':3}})
    True

    >>> _cmp({'a':{'b':{'c':[1,2,3]}}},{'a':{'b':{'c':'d'}}})
    False

    >>> _cmp({'num1':1,'stringAbc':'aBc','objY':{'num2':2}}, {"num1":1,"stringAbc":"Abc","objY":{"num2":2}},True)
    True
    '''
    if not type(data) == type(moredata): return False
    elif isinstance(data,dict):
        if not len(data) == len(moredata): return False
        if not set(data.keys()) == set(moredata.keys()): return False
        for key in data.keys():
            if key not in moredata: return False
            if not _cmp(data[key],moredata[key],ignore_case): return False
    elif isinstance(data,list):
        if not len(data) == len(moredata): return False
        for value in data:
            ok = False
            for morevalue in moredata:
                if _cmp(value,morevalue,ignore_case): ok = True
            if not ok: return False
    elif ignore_case and isinstance(data,str):
        if not data.upper() == moredata.upper(): return False
    else:
        if not data == moredata: return False
    return True
def pred_test(doc,pred):
    assert isinstance(pred,dict),"predicate should be an obj"
    assert 'path' in pred, "no path"
    path = pred['path']
    assert 'value' in pred, "no value"
    value = pred['value']
    fetched = ptrget(path,doc)
    #print('test',fetched,'vs',value)
    if 'ignore_case' in pred:
        ignore_case = pred['ignore_case']
    else:
        ignore_case = False
    assert _cmp(fetched,value,ignore_case), "not the same"
    return doc

def pred_ends(doc,pred):
    #todo add ignore_case
    assert isinstance(pred,dict),"predicate should be a dict"
    assert 'path' in pred, "no path"
    assert 'value' in pred, "no value"
    path = pred['path']
    value = pred['value']
    assert isinstance(value,(str,unicode)), "expect value to be a string"
    fetched = ptrget(path,doc)
    assert isinstance(fetched,(str,unicode)), "expect string at %s" % path
    if 'ignore_case' in pred and pred['ignore_case']:
        value = value.upper()
        fetched = fetched.upper()
    assert fetched.endswith(value), "dont endwith"
    return doc

def pred_starts(doc,pred):
    #todo add ignore_case
    assert isinstance(pred,dict), "predicate should be a dict"
    assert 'path' in pred, "no path"
    assert 'value' in pred, "no value"
    path = pred['path']
    value = pred['value']
    assert isinstance(value,(str,unicode)), "value should be string"
    fetched = ptrget(path,doc)
    assert isinstance(fetched,(str,unicode)), "expected a string at %s" % path
    if 'ignore_case' in pred and pred['ignore_case']:
        value = value.upper()
        fetched = fetched.upper()
    assert fetched.startswith(value), "dont start with"
    return doc

def pred_defined(doc,pred):
    assert isinstance(pred,dict), "predicate should be a dict"
    assert 'path' in pred, "no path"
    found = None
    try:
        fetched  = ptrget(pred['path'],doc)
        found = True
    except:
        found = False
    assert found, "not defined"
    return doc

def pred_undefined(doc,pred):
    assert isinstance(pred,dict), "predicate should be dict"
    assert 'path' in pred, "no path"
    found = None
    try:
        fetched = ptrget(pred['path'],doc)
        found = True
    except:
        found = False
    assert not found, "seams defined after all"
    return doc

def pred_less(doc,pred):
    assert isinstance(pred,dict), "predicate should be a dict"
    assert 'path' in pred, "no path"
    assert 'value' in pred, "no value"
    value = pred['value']
    assert isinstance(value,int) or isinstance(value,float), "expected value number"
    path = pred['path']
    less = ptrget(path,doc)
    assert isinstance(less,int) or isinstance(less,float), "expected number at %s" % path
    assert less < value, "more or equal"
    return doc

def pred_more(doc,pred):
    assert isinstance(pred,dict), "predicate should be dict"
    assert 'path' in pred, "no path"
    assert 'value' in pred, "no pred"
    value = pred['value']
    assert isinstance(value,int) or isinstance(value,float), "expect value to be number"
    path = pred['path']
    more = ptrget(path,doc)
    assert isinstance(more,int) or isinstance(more,float),"expected number at %s"%path
    assert more > value, "less or equal"
    return doc

#regex from https://github.com/MalcolmDwyer/json-predicate#type
_date_regex = r'^\d{4}-\d{2}-\d{2}$'
_date_time_regex = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?((?:[\+\-]\d{2}:\d{2})|Z)$'
_time_regex = r'^\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?((?:[\+\-]\d{2}:\d{2})|Z)$'
_lang_regex = r'^[a-z]{2,3}(?:-[A-Z]{2,3}(?:-[a-zA-Z]{4})?)?$'
_lang_range_regex = r'^\*|[A-Z]{1,8}(?:-[\*A-Za-z0-9]{1,8})?$'
#mine own crop
_iri_regex = r'^https?:/(/.+)+(#.+)?$'
_absolute_iri_regex = r'^https?:/(/.+)+?$'

def pred_type(doc,pred):
    assert isinstance(pred,dict)
    assert 'path' in pred
    assert 'value' in pred
    ok = False
    path = pred['path']
    value = pred['value']
    try:
        fetched = ptrget(path,doc)
        ok = (not fetched and value == 'null') or \
             (isinstance(fetched,int) and value == 'number') or \
             (isinstance(fetched,float) and value == 'number') or \
             (isinstance(fetched,str) and value == 'string') or \
             (isinstance(fetched,bool) and value == 'boolean') or \
             (isinstance(fetched,list) and value == 'array') or \
             (isinstance(fetched,dict) and value == 'object') or \
             (isinstance(fetched,str) and value == 'date' and re.match(_date_regex,fetched)) or \
             (isinstance(fetched,str) and value == 'date-time' and re.match(_date_time_regex,fetched)) or \
             (isinstance(fetched,str) and value == 'time' and re.match(_time_regex,fetched)) or \
             (isinstance(fetched,str) and value == 'lang' and re.match(_lang_regex,fetched)) or \
             (isinstance(fetched,str) and value == 'lang-range' and re.match(_lang_range_regex,fetched)) or \
             (isinstance(fetched,str) and value == 'iri' and re.match(_iri_regex,fetched)) or \
             (isinstance(fetched,str) and value == 'absolute-iri' and re.match(_iri_regex,fetched)) or \
             (fetched in [0,1] and value == 'boolean')
    except:
        ok = 'undefined' == value
    assert ok, "not your type"
    return doc

#second order are recursive
def pred_and(doc,pred):
    assert isinstance(pred,dict), "predicate should be dict"
    assert 'apply' in pred, "no apply"
    for sub in pred['apply']:
        assert doc == patch(doc,sub,predicate_op), "%s yield false" % sub
    return doc

def pred_or(doc,pred):
    assert isinstance(pred,dict), "predicate should be dict"
    assert 'apply' in pred, "no apply"
    for sub in pred['apply']:
        try:
            if doc == patch(doc,sub,predicate_op):
                return doc
        except:
            pass
    raise AssertionError('none succeed')

def pred_not(doc,pred):
    assert isinstance(pred,dict), "predicate should be dict"
    assert 'apply' in pred, "no apply"
    assert isinstance(pred['apply'],list)
    for sub in pred['apply']:
        try:
            dare = patch(doc,sub,predicate_op)
        except:
            dare = None
        assert dare != doc,"one succeed"
    return doc



predicate_op = {
    'contains' : pred_contains,
    'matches' : pred_matches,
    'in' : pred_in,
    'test' : pred_test,
    'ends' : pred_ends,
    'starts' : pred_starts,
    'defined' : pred_defined,
    'undefined' : pred_undefined,
    'less' : pred_less,
    'more' : pred_more,
    'type' : pred_type,

    'and' : pred_and,
    'or' : pred_or,
    'not' : pred_not
}

def test(doc,pred):
    '''
    TEST FROM https://github.com/MalcolmDwyer/json-predicate/blob/master/test/test.js
    >>> test(predicate_ex,{'foo': 'bar'})
    False

    >>> test(predicate_ex,{'op':'contains','value':'AB','path':'/stringABC'})
    True

    >>> test(predicate_ex,{'op':'contains','value':'XY','path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'contains','value':'MN','path':'/objA/objB/stringMNO'})
    True

    >>> test(predicate_ex,{'op':'contains','value':'AB','path':'/objA/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'contains','value':'xy','path':'/objA/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'contains','value':'xy','path':'/objA/stringXYZ','ignore_case':True})
    True

    >>> test(predicate_ex,{'op':'matches','value':"[\\w\\s]*",'path':'/num1'})
    False

    >>> test(predicate_ex,{'op':'matches','value':"[\\w\\s]*",'path':'/null1'})
    False

    >>> test(predicate_ex,{'op':'matches','value':"[\\w\\s]*",'path':'/objA'})
    False

    >>> test(predicate_ex,{'op':'matches','value':"[\\w\\s]*",'path':'/arrayA'})
    False

    >>> test(predicate_ex,{'op':'matches','value':'\\\\','path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'matches','value':1,'path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'matches','value':{'a':1},'path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'matches','value':['a', 'b', 'c'],'path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'matches','value':"[A-Z]*",'path':'/stringABC'})
    True

    >>> test(predicate_ex,{'op':'matches','value':"aBc",'path':'/stringABC', 'ignore_case':True})
    True

    >>> test(predicate_ex,{'op':'matches','value':"aBc*",'path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'matches','value':"[A-Z]+",'path':'/stringABC'})
    True

    >>> test(predicate_ex,{'op':'in','value':['foo', 'ABC', 'bar'],'path':'/stringABC'})
    True

    >>> test(predicate_ex,{'op':'in','value':['foo', 'aBc', 'bar'],'path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'in','value':['foo', 'aBc', 'bar'],'path':'/stringABC','ignore_case':True})
    True

    >>> test(predicate_ex,{'op':'in','value':['foo', {'num2':2}, 'bar'],'path':'/objX/objY'})
    True

    >>> test(predicate_ex,{'op':'in','value':['foo',{'num1':1,'stringAbc':'Abc','objY':{'num2':2}},'bar'],'path':'/objX'})
    True

    >>> test(predicate_ex,{'op':'in','value':[{'foo': 'foo'}, 2, 'bar'],'path':'/objA/num2'})
    True

    >>> test(predicate_ex,{'op':'in','value':'ABC','path':'/stringA'})
    False

    >>> test(predicate_ex,{'op':'in','value':['foo',{'num1':1,'stringAbc':'aBc','objY':{'num2':2}},'bar'],'path':'/objX','ignore_case':True})
    True

    >>> test(predicate_ex,{'op':'in','value':['foo',{'num1':1,'stringAbc':'aBc','objY':{'num2':2}},'bar'],'path':'/objX'})
    False

    >>> test(predicate_ex,{'op':'test','value':'ABC','path':'/stringABC'})
    True

    >>> test(predicate_ex,{'op':'test','value':'aBc','path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'test','value':'aBc','path':'/stringABC','ignore_case':True})
    True

    >>> test(predicate_ex,{'op':'test','value':{'num2':2},'path':'/objX/objY'})
    True

    >>> test(predicate_ex,{'op':'test','value':{'num1':1, 'stringAbc': 'Abc', 'objY': {'num2':2}},'path':'/objX'})
    True

    >>> test(predicate_ex,{'op':'test','value':2,'path':'/objA/num2'})
    True

    >>> test(predicate_ex,{'op':'test','value':['a', 'b', 'c'],'path':'/arrayA'})
    True

    >>> test(predicate_ex,{'op':'test','value':['a', 'b', 'c', 'd'],'path':'/arrayA'})
    False

    >>> test(predicate_ex,{'op':'test','value':['a', 'b'],'path':'/arrayA'})
    False

    >>> test(predicate_ex,{'op':'test','value':'abcd','path':'/arrayA'})
    False

    >>> test(predicate_ex,{'op':'test','value':['a', 'b', 'c'],'path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'test','value':{'num1':1,'stringAbc':'aBc','objY':{'num2':2}},'path':'/objX','ignore_case':True})
    True

    >>> test(predicate_ex,{'op':'test','value':{'num1':1,'stringAbc':'aBc','objY':{'num2':2}},'path':'/objX'})
    False

    >>> test(predicate_ex,{'op':'ends','value':'BC','path':'/stringABC'})
    True

    >>> test(predicate_ex,{'op':'ends','value':'AB','path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'ends','value':'NO','path':'/stringMNO'})
    True

    >>> test(predicate_ex,{'op':'ends','value':'XY','path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'ends','value':'yz','path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'ends','value':'yz','path':'/stringXYZ','ignore_case':True})
    True

    >>> test(predicate_ex,{'op':'starts','value':'AB','path':'/stringABC'})
    True

    >>> test(predicate_ex,{'op':'starts','value':'BC','path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'starts','value':'MN','path':'/stringMNO'})
    True

    >>> test(predicate_ex,{'op':'starts','value':'YZ','path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'starts','value':'xy','path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'starts','value':'xy','path':'/stringXYZ','ignore_case':True})
    True

    op defined line 519 https://github.com/MalcolmDwyer/json-predicate/blob/master/test/test.js
    >>> test(predicate_ex,{'op':'defined','path':'/num1'})
    True

    >>> test(predicate_ex,{'op':'defined','path':'/null1'})
    True

    >>> test(predicate_ex,{'op':'defined','path':'/not_a_key'})
    False

    >>> test(predicate_ex,{'op':'defined','path':'/objA/objB/num3'})
    True

    >>> test(predicate_ex,{'op':'defined','path':'/objA/objB/null3'})
    True

    >>> test(predicate_ex,{'op':'defined','path':'/objA/objB/not_a_key'})
    False

    op undefined line 564 https://github.com/MalcolmDwyer/json-predicate/blob/master/test/test.js
    >>> test(predicate_ex,{'op':'undefined','path':'/not_a_key'})
    True

    >>> test(predicate_ex,{'op':'undefined','path':'/objA/not_a_key'})
    True

    >>> test(predicate_ex,{'op':'undefined','path':'/num1'})
    False

    >>> test(predicate_ex,{'op':'undefined','path':'/null1'})
    False

    >>> test(predicate_ex,{'op':'undefined','path':'/objA/objB/num3'})
    False

    >>> test(predicate_ex,{'op':'undefined','path':'/objA/objB/null3'})
    False

    op less line 608 https://github.com/MalcolmDwyer/json-predicate/blob/master/test/test.js
    return false for non-numeric comparisons
    >>> test(predicate_ex,{'op':'less','value':'XYZ','path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'less','value':'ABC','path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'less','value':['a', 'b'],'path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'less','value':{'a': 'foo', 'b': 'bar'},'path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'less','value':4,'path':'/objA/objB/num3'})
    True

    >>> test(predicate_ex,{'op':'less','value':2,'path':'/objA/objB/num3'})
    False

    >>> test(predicate_ex,{'op':'less','value':3,'path':'/objA/objB/num3'})
    False

    op more line 661 https://github.com/MalcolmDwyer/json-predicate/blob/master/test/test.js
    return false for non-numeric comparisons
    >>> test(predicate_ex,{'op':'more','value':'XYZ','path':'/stringABC'})
    False

    >>> test(predicate_ex,{'op':'more','value':'ABC','path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'more','value':['a', 'b'],'path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'more','value':{'a': 'foo', 'b': 'bar'},'path':'/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'more','value':4,'path':'/objA/objB/num3'})
    False

    >>> test(predicate_ex,{'op':'more','value':2,'path':'/objA/objB/num3'})
    True

    >>> test(predicate_ex,{'op':'more','value':3,'path':'/objA/objB/num3'})
    False

    op type line 715 https://github.com/MalcolmDwyer/json-predicate/blob/master/test/test.js
    >>> test(predicate_ex,{'op':'type','value':'number','path':'/objA/num2'})
    True

    >>> test(predicate_ex,{'op':'type','value':'number','path':'/objA/stringXYZ'})
    False

    >>> test(predicate_ex,{'op':'type','value':'string','path':'/objA/stringXYZ'})
    True

    >>> test(predicate_ex,{'op':'type','value':'string','path':'/objA/num2'})
    False

    >>> test(predicate_ex,{'op':'type','value':'boolean','path':'/objA/boolT'})
    True

    >>> test(predicate_ex,{'op':'type','value':'boolean','path':'/objA/boolF'})
    True

    added this one myself, seams boolean are number
    >>> test(predicate_ex,{'op':'type','value':'number','path':'/objA/boolT'})
    True

    >>> test(predicate_ex,{'op':'type','value':'boolean','path':'/objA/num2'})
    False

    >>> test(predicate_ex,{'op':'type','value':'object','path':'/objA/objB'})
    True

    >>> test(predicate_ex,{'op':'type','value':'object','path':'/objA/num2'})
    False

    >>> test(predicate_ex,{'op':'type','value':'array','path':'/arrayA'})
    True

    >>> test(predicate_ex,{'op':'type','value':'array','path':'/objA/num2'})
    False

    >>> test(predicate_ex,{'op':'type','value':'null','path':'/objA/null2'})
    True

    >>> test(predicate_ex,{'op':'type','value':'null','path':'/objA/num2'})
    False

    >>> test(predicate_ex,{'op':'type','value':'undefined','path':'/objA/not_a_thing'})
    True

    >>> test(predicate_ex,{'op':'type','value':'undefined','path':'/objA/num2'})
    False

    test from https://github.com/MalcolmDwyer/json-predicate/blob/master/test/test.js line 935
    recursive predicate
    >>> test(predicate_ex,{'op':'and','apply':[{'op':'defined','path':'/stringA'},{'op': 'defined','path': '/stringABC'}]})
    True

    >>> test(predicate_ex,{'op':'and','apply':[{'op':'defined','path':'/objA/stringX'},{'op': 'defined','path': '/objA/stringXYZ'}]})
    True

    >>> test(predicate_ex,{'op':'and','apply':[{'op':'defined','path':'/stringX'},{'op': 'defined','path': '/not_real'}]})
    False

    >>> test(predicate_ex,{'op':'or','apply':[{'op':'defined','path':'/not_real_thing'},{'op': 'defined','path': '/stringABC'}]})
    True

    >>> test(predicate_ex,{'op':'or','apply':[{'op':'defined','path':'/objA/not_real_thing'},{'op': 'defined','path': '/objA/stringXYZ'}]})
    True

    >>> test(predicate_ex,{'op':'or','apply':[{'op':'defined','path':'/not_real'},{'op': 'defined','path': '/not_real2'}]})
    False

    >>> test(predicate_ex,{'op':'not','apply':[{'op':'defined','path':'/not_real1'},{'op': 'defined','path': '/not_real2'}]})
    True

    >>> test(predicate_ex,{'op':'not','apply':[{'op':'defined','path':'/objA/not_real1'},{'op': 'defined','path': '/objA/not_real2'}]})
    True

    >>> test(predicate_ex,{'op':'defined','path':'/stringA'})
    True

    >>> test(predicate_ex,{'op':'defined','path': '/not_real'})
    False

    >>> test(predicate_ex,{'op':'not','apply':[{'op':'defined','path':'/stringA'},{'op': 'defined','path': '/not_real'}]})
    False

    nested predicate
    >>> nested = {"op":"or","apply":[{"op":"not","apply":[{"op":"defined","path":"/a/b/c"},{"op":"starts","path":"/a/b/c","value":"f"}]},{"op":"not","apply":[{"op":"defined","path":"/a/b/d"},{"op":"type","path":"/a/b/d","value":"number"}]}]}
    >>> test({},nested)
    True

    >>> test({'a':{'b':{'c':'f','d':1}}},nested)
    False

    >>> test({'a':{'b':{'d':1}}},nested)
    True


    not in spec .... yes they are, you must fix this http://tools.ietf.org/id/draft-snell-json-test-01.html#rfc.section.2.2.10
    "number", "string", "boolean", "object", "array", "null", "undefined", "date", "date-time", "time", "lang", "lang-range", "iri" or "absolute-iri".
    from https://github.com/MalcolmDwyer/json-predicate/blob/master/test/test.js line 817 to 934
    >>> test(predicate_ex,{'op':'type','value':'date','path':'/objA/date'})
    True

    >>> test(predicate_ex,{'op':'type','value':'date','path':'/objA/dateTime'})
    False

    >>> test(predicate_ex,{'op':'type','value':'time','path':'/objA/timeZ'})
    True

    >>> test(predicate_ex,{'op':'type','value':'time','path':'/objA/timeOffset'})
    True

    r
    >>> test(predicate_ex,{'op':'type','value':'time','path':'/objA/dateTime'})
    False

    >>> test(predicate_ex,{'op':'type','value':'date-time','path':'/objA/dateTime'})
    True

    >>> test(predicate_ex,{'op':'type','value':'date-time','path':'/objA/dateTimeOffset'})
    True

    >>> test(predicate_ex,{'op':'type','value':'date-time','path':'/objA/date'})
    False

    >>> test(predicate_ex,{'op':'type','value':'lang','path':'/objA/lang'})
    True

    >>> test(predicate_ex,{'op':'type','value':'lang','path':'/objA/num2'})
    False

    >>> test(predicate_ex,{'op':'type','value':'lang-range','path':'/objA/langRange'})
    True

    >>> test(predicate_ex,{'op':'type','value':'lang-range','path':'/objA/langRange2'})
    True

    >>> test(predicate_ex,{'op':'type','value':'lang-range','path':'/objA/langRange3'})
    True

    >>> test(predicate_ex,{'op':'type','value':'lang-range','path':'/objA/num2'})
    False

    >>> test(predicate_ex,{'op':'type','value':'iri','path':'/objA/iri'})
    True

    >>> test(predicate_ex,{'op':'type','value':'absolute-iri','path':'/objA/num2'})
    False

    >>> test(predicate_ex,{'op':'type','value':'absolute-iri','path':'/objA/absoluteIri'})
    True

    >>> test(predicate_ex,{'op':'type','value':'iri','path':'/objA/num2'})
    False


    '''
    try:
        result = patch(doc,pred,predicate_op)
    except:
        return False
    return result == doc

'''
json-patch related spec
json-hal:
    https://tools.ietf.org/html/draft-kelly-json-hal-05
    i am not sure what it is and how it differenciate from json-ref
    it seam to define rest api... 

more functinality
sql query:
    https://www.npmjs.com/package/json-sql
    https://github.com/2do2go/json-sql
json math:
    https://www.npmjs.com/package/json-math
    https://github.com/ChrisTheBaron/node-json-math

'''
def jmath(doc,patch):
    '''
    math operation spec
    [+ - * / ^ sin cos tan ]

    >>> jmath(0,{'op':'+','path':'/','value':1})
    1
    
    >>> jmath([0,1],{'op':'+','path':'/','value':[1,1]})
    [1,2]
    
    >>> jmath(0,{'op':'+','path':'/','value':-1})
    -1
    
    >>> jmath([0],{'op':'+','path':'/','value':[1]})
    1
    
    >>> jmath([0,1],{'op':'+','path':'/','value':[1,"error"]})
    AssertionError ...
    
    >>> jmath([0,1],{'op':'+','path':'/','value':[3]})
    AssertionError ...
    
    >>> jmath(0,{'op':'-','path':'/','value':1})
    -1
    
    >>> jmath([0,1],{'op':'-','path':'/','value':[1,1]})
    [-1,0]
    
    >>> jmath(0,{'op':'-','path':'/','value':-1})
    1
    
    >>> jmath([0],{'op':'-','path':'/','value':[1]})
    -1
    
    >>> jmath([0,1],{'op':'-','path':'/','value':[1,"error"]})
    AssertionError ...
    
    >>> jmath(0,{'op':'*','path':'/','value':1})
    0
    
    >>> jmath([0,1],{'op':'*','path':'/','value':[[3,4]]})
    [11]
    
    >>> jmath(0,{'op':'*','path':'/','value':-1})
    0
    
    >>> jmath([0],{'op':'*','path':'/','value':[1]})
    0
    
    >>> jmath([0,1],{'op':'*','path':'/','value':[1,"error"]})
    AssertionError ...
    
    '''
    return doc
'''
Future:
    json agent
      patch
      predicate
      reference
      patchnpredicate
      merge (according to draft)
'''

def _merge(params):
    return merge(params)

def _predicate(params):
    if isinstance(params,list):
        return [ _predicate(param) for param in params if params    ]
    assert isinstance(params,dict)
    assert 'doc' in params
    assert 'patch' in params
    return patch(params['doc'],params['patch'],predicate_op)

def _ref(params):
    def httpGetJson(url):
        import json, urllib2
        txt = urllib2.urlopen(url)
        json = json.load(txt)
        return json
    return reference(params,httpGetJson)

def _patch(params):
    if isinstance(params,list):
        return [ _patch(param) for param in params if params    ]
    assert isinstance(params,dict)
    assert 'doc' in params
    assert 'patch' in params
    ptch = params['patch']
    doc = params['doc']
    result = patch(doc,ptch,patch_op)
    return result

_all_op = copy.deepcopy(patch_op)
_all_op.update(predicate_op)
_all_op.update({'json-merge':merge})
def _all(params):
    '''
    first parse reference
    then apply patch and predicate
    '''
    params = reference(params)
    if isinstance(params,list):
        return [ _all(param) for param in params if params    ]
    assert isinstance(params,dict)
    assert 'doc' in params
    assert 'patch' in params
    return patch(params['doc'],params['patch'],_all_op)

agentProcDir = {
    'json-merge' : _merge,
    'json-predicate' : _predicate,
    'json-patch' : _patch,
    'json-reference' : _ref,
    'json-all' : _all
}

if __name__ == '__main__':
    import doctest
    doctest.testmod()
