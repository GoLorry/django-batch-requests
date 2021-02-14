from django.contrib.auth import authenticate

from rest_framework.authentication import BaseAuthentication


class BatchAuthentication(BaseAuthentication):
    '''
    Similar to session authentication. Authenticates users already authenticated by
    batch_requests.
    '''

    def authenticate(self, request):
        '''
        Returns a `User` if the batch request set a logged in user.
        Otherwise returns `None`.
        '''

        # Get the session-based user from the underlying HttpRequest object
        user = getattr(request._request, 'batch_user', None)

        if not user or not user.is_active:
            return None
        return (user, None)