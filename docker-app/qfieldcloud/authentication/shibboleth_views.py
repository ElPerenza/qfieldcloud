import hashlib
import json

from django.conf import settings
from django.contrib.auth import authenticate, login, logout
from django.http import JsonResponse
from django.shortcuts import redirect
from rest_framework.permissions import AllowAny
from rest_framework.views import APIView


class ShibbolethView(APIView):

    permission_classes = (AllowAny,)

    def get(self, request, **kwargs):

        nxt: str = request.GET.get("next", None)
        username_md5: str =  request.headers['Shib-Identita-Nome'] + '/' + request.headers['Shib-Identita-Cognome'] + '/' +  request.headers['Shib-Identita-Codicefiscale']
        username_md5_hash = hashlib.md5(username_md5.lower().encode()).hexdigest()
        default_user_pass = 'mypass001'
        user = authenticate(request, username=username_md5_hash, password=default_user_pass)

        if hasattr(settings, 'SHIBBOLETH_LIV2_SPID_COOKIE') and settings.SHIBBOLETH_LIV2_SPID_COOKIE:
            ShibbolethLogoutView.write(settings.SHIBBOLETH_LIV2_SPID_COOKIE)
        else:
            ShibbolethLogoutView.write('settings.SHIBBOLETH_LIV2_SPID_COOKIE not found')

        if hasattr(settings, 'SHIBBOLETH_LIV2_SPID_LOGOUT') and settings.SHIBBOLETH_LIV2_SPID_LOGOUT:
            ShibbolethLogoutView.write(settings.SHIBBOLETH_LIV2_SPID_LOGOUT)
        else:
            ShibbolethLogoutView.write('settings.SHIBBOLETH_LIV2_SPID_LOGOUT not found')

        if hasattr(settings, 'SHIBBOLETH_LIV1_SPID_COOKIE') and settings.SHIBBOLETH_LIV1_SPID_COOKIE:
            ShibbolethLogoutView.write(settings.SHIBBOLETH_LIV1_SPID_COOKIE)
        else:
            ShibbolethLogoutView.write('settings.SHIBBOLETH_LIV1_SPID_COOKIE not found')

        if hasattr(settings, 'SHIBBOLETH_LIV1_SPID_LOGOUT') and settings.SHIBBOLETH_LIV1_SPID_LOGOUT:
            ShibbolethLogoutView.write(settings.SHIBBOLETH_LIV1_SPID_LOGOUT)
        else:
            ShibbolethLogoutView.write('settings.SHIBBOLETH_LIV1_SPID_LOGOUT not found')

        if user is not None:
            login(request, user)
            if nxt is not None:
                return redirect(nxt)
        else:
            return JsonResponse(json.dumps(username_md5 + ' is unauthorized'), safe=False)
        

class ShibbolethLogoutView(APIView):

    permission_classes = (AllowAny,)

    @classmethod
    def write(cls, text: str):
        if text is not None:
            with open('/tmp/shib.log', 'a') as f:
                f.write(text)
                f.write('\n')

    def get(self, request, **kwargs):
        if getattr(settings, "ACCOUNT_LOGOUT_ON_GET", False):
            response = self.post(request, **kwargs)
        else:
            response = self.http_method_not_allowed(request, **kwargs)
        return self.finalize_response(request, response, **kwargs)
    
    def post(self, request, **kwargs):
        ShibbolethLogoutView.write('ShibbolethLogoutView::get -> redirecting logout')
        
        if hasattr(settings, 'SHIBBOLETH_COOKIES') and hasattr(settings, 'SHIBBOLETH_LOGOUTS') and len(settings.SHIBBOLETH_COOKIES) == len(settings.SHIBBOLETH_LOGOUTS):
            shibboleth_cookies = settings.SHIBBOLETH_COOKIES
            shibboleth_logouts = settings.SHIBBOLETH_LOGOUTS

            for i in range (len(shibboleth_cookies)):
                # print('cookie', shibboleth_cookies[i])
                if request.headers.get('Cookie').find(shibboleth_cookies[i]) != -1:
                    ShibbolethLogoutView.write('ShibbolethLogoutView::get -> found ' + shibboleth_cookies[i])
                    logout(request)
                    # print('logout', shibboleth_logouts[i])
                    return redirect(shibboleth_logouts[i])
                
        ShibbolethLogoutView.write('ShibbolethLogoutView::get -> no known header found')
        logout(request)
        return redirect('/')


class ShibbolethInfoView(APIView):

    def get(self, request, **kwargs):

        headers = request.headers.__dict__
        username_md5 =  request.headers['Shib-Identita-Nome'] + '/' + request.headers['Shib-Identita-Cognome'] + '/' +  request.headers['Shib-Identita-Codicefiscale']
        spidheader = ''
        spidheadervalue = ''
        shibheader = ''
        shibheadervalue = ''

        if request.headers.get('Cookie').find(settings.SHIBBOLETH_LIV2_SPID_COOKIE) != -1:
            spidheader = 'true'
            spidheadervalue = settings.SHIBBOLETH_LIV2_SPID_LOGOUT
        elif request.headers.get('Cookie').find(settings.SHIBBOLETH_LIV1_WRUP_COOKIE) != -1:
            shibheader = 'true'
            shibheadervalue = settings.SHIBBOLETH_LIV1_WRUP_LOGOUT

        dictspid = json.loads('{"spidheader":"' + spidheader + '", "spidheadervalue":"' + spidheadervalue + '"}')
        dictshib = json.loads('{"shibheader":"' + shibheader + '", "shibheadervalue":"' + shibheadervalue + '"}')
        dictmd5 = json.loads('{"username_md5":"' + username_md5.lower() + '", "username_md5_hash":"' + hashlib.md5(username_md5.lower().encode()).hexdigest() + '"}')
        headers.update(dictspid)
        headers.update(dictshib)
        headers.update(dictmd5)

        current_user = request.user
        userdict = {}
        userdir = dir(current_user)
        i = 1
        for item in userdir:
            userdict[i] = item
            i += 1
        headers.update(userdict)

        context = {}
        for setting in dir(settings):
            if setting.isupper():
                context[setting] = getattr(settings, setting)
        headers.update(context)
        
        return JsonResponse(headers, safe=False)
    