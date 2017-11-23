'''
surely import requests

request
{
    method : get
    url : '',
    params : {}
    auth : (usr,pass),
    cookies : {},
    timeout : 0,
    payload : ...,
    headers : {
        'content-encoding': 'gzip',
        'transfer-encoding': 'chunked',
        'connection': 'close',
        'server': 'nginx/1.0.4',
        'x-runtime': '148ms',
        'etag': '"e1ca502697e5c9317743dc078f67693f"',
        'content-type': 'application/json'
    }
}

response
{
    status_code : 
    reason :
    payload :
    encoding : 

}
'''

import requests

def _get_header(params):
    if 'header' in params:
        header = params['header']
    else:
        header = None
    return header

def _get_params_uri(params):
    if 'params' in params:
        tmp = params['params']
    else:
        tmp = {}
    return tmp

def _get_reponse(response):
    r = {
        'status_code' : response.status_code,
        'encoding' : response.encoding,
        'headers' : dict(response.headers),
        'cookies' : dict(response.cookies)
    }
    if response.headers['Content-Type'] == 'application/json':
        r['json'] = response.json()
    else:
        r['text'] = response.text
    return r

def _get_cookies_params(params):
    if 'cookies' in params:
        cookies = params['cookies']
    else:
        cookies = None
    return cookies

def _get_auth(params):
    if 'auth' in params:
        assert isinstance(params['auth'],dict),"auth must be a dict"
        if not 'type' in params['auth'] or params['auth']['type'] == 'HTTP Basic Auth':
            assert 'user' in params['auth'], "user is required in http basic authentification"
            assert 'pass' in params['auth'], "password is required in http basic authentification"
            auth = (params['auth']['user'],params['auth']['pass'])
        elif params['auth']['type'] == 'OAuth1':
            from requests_oauthlib import OAuth1
            assert 'key' in params['auth'], "api key is required"
            assert 'secret' in params['auth'], "secret is also required"
            assert 'user' in params['auth'], "user is required"
            assert 'pass' in params['auth'], "password is required"
            auth = OAuth1(params['auth']['key'], \
                          params['auth']['secret'], \
                          params['auth']['user'], \
                          params['auth']['pass'])
        else:
            raise Exception("authentification methode not supported %s"%params['auth']['type'])
    else:
        auth = None
    return auth

def _get(params):
    assert 'url' in params, "url required"
    response = requests.get(params['url'],
                            params=_get_params_uri(params),
                            auth=_get_auth(params),
                            cookies=_get_cookies_params(params),
                            headers=_get_header(params))
    if response.status_code == 200:
        return _get_reponse(response)
    else:
        raise Exception("http returned <%s> because %s"%(response.status_code,response.reason))

def _post(params):
    assert 'url' in params, "url required"
    assert 'data' in params, "data required"
    response = requests.post(params['url'],
                            params=_get_params_uri(params),
                            auth=_get_auth(params),
                            data=params['data'],
                            cookies=_get_cookies_params(params),
                            headers=_get_header(params))
    if response.status_code in [200,201]:
        return _get_reponse(response)
    else:
        raise Exception("http returned <%s> because %s"%(response.status_code,response.reason))

def _put(params):
    assert 'url' in params, "url required"
    assert 'data' in params, "data required"
    response = requests.put(params['url'],
                            params=_get_params_uri(params),
                            auth=_get_auth(params),
                            data=params['data'],
                            cookies=_get_cookies_params(params),
                            headers=_get_header(params))
    if response.status_code in [200,201,202]:
        return _get_reponse(response)
    else:
        raise Exception("http returned <%s> because %s"%(response.status_code,response.reason))

def _delete(params):
    assert 'url' in params, "url required"
    response = requests.delete(params['url'],
                            params=_get_params_uri(params),
                            auth=_get_auth(params),
                            cookies=_get_cookies_params(params),
                            headers=_get_header(params))
    if response.status_code in [200,201,204]:
        return _get_reponse(response)
    else:
        raise Exception("http returned <%s> because %s"%(response.status_code,response.reason))

def _nope(params):
    raise Exception("not implemented yet")

agentProcDir = {
    "GET" : _get,
    "POST" : _post,
    "PUT" : _put,
    "DELETE" : _delete
}

if __name__ == '__main__':
    import doclan
    args = doclan.doclanarg(agentProcDir)
