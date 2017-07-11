"""
@summary: Module containing user services for basic authentication
"""
import cherrypy
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmWebServer.common.lmconstants import REFERER_KEY, SESSION_KEY
from LmWebServer.services.api.v2.base import LmService


# .............................................................................
@cherrypy.expose
class UserLogin(LmService):
   """
   @summary: This is the user login service.  Sending a GET request will return
                a login page.  POSTing will attempt to login with the user's
                credentials
   """
   # ................................
   def GET(self):
      """
      @summary: Present the user with a login page if they are not already 
                   logged in.  If they are, return their user name
      """
      # Check if the user is logged in
      if cherrypy.session.user != PUBLIC_USER:
         # Already logged in
         return "Welcome {}".format(cherrypy.session.user)
      else:
         # Return login page
         return _get_login_page()

   # ................................
   def POST(self, userId, pword):
      """
      @summary: Attempt to log in using the provided credentials
      """
      if userId is None or pword is None:
         raise cherrypy.HTTPError(400, "Must provide user name and password")
      
      refererPage = None
      
      try:
         cookie = cherrypy.request.cookie
         if cookie.has_key(REFERER_KEY):
            refererPage = cookie[REFERER_KEY].value
         else:
            refererPage = cherrypy.request.headers['referer']
            cookie = cherrypy.response.cookie
            cookie[REFERER_KEY] = refererPage
            cookie[REFERER_KEY]['path'] = '/api/login'
            cookie[REFERER_KEY]['max-age'] = 30
            cookie[REFERER_KEY]['version'] = 1
      except:
         pass
      
      user = self.scribe.getUser(userId)
      if user is not None and user.checkPassword(pword):
         # Provided correct credentials
         cherrypy.session.regenerate()
         cherrypy.session[SESSION_KEY] = cherrypy.request.login = userId
         cookie = cherrypy.response.cookie
         cookie[REFERER_KEY] = refererPage
         cookie[REFERER_KEY]['expires'] = 0
         raise cherrypy.HTTPRedirect(refererPage or '/')
      else:
         raise cherrypy.HTTPError(403, 'Invalid username / password combination')

# .............................................................................
@cherrypy.expose
class UserLogout(LmService):
   """
   @summary: Log the user out of the system
   """
   # ................................
   def GET(self):
      cherrypy.lib.sessions.expire()
      cherrypy.session[SESSION_KEY] = cherrypy.request.login = None
      
      raise cherrypy.HTTPRedirect('/api/login')
   
# .............................................................................
@cherrypy.expose
class UserSignUp(LmService):
   """
   @summary: Service to create a new user
   """
   # ................................
   def GET(self):
      """
      @summary: Present a new user form
      """
      return _get_signup_page()
   
   # ................................
   def POST(self, userId, email, firstName, pword1, lastName=None, 
            institution=None, address1=None, address2=None, address3=None, 
            phone=None):
      
      if not _verify_length(userId, maxLength=20, minLength=5):
         raise cherrypy.HTTPError(400, 
                               'User ID must have between 5 and 20 characters')
      if not _verify_length(firstName, minLength=2, maxLength=50):
         raise cherrypy.HTTPError(400, 
                            'First name must have between 2 and 50 characters')
      if not _verify_length(lastName, minLength=2, maxLength=50):
         raise cherrypy.HTTPError(400, 
                             'Last name must have between 2 and 50 characters')
      if phone is not None and len(phone) > 0 and not _verify_length(phone, minLength=10, maxLength=20):
         raise cherrypy.HTTPError(400, 
                         'Phone number must have between 10 and 20 characters')
      if not _verify_length(email, minLength=9, maxLength=64):
         raise cherrypy.HTTPError(400, 
                                 'Email must have between 9 and 64 characters')
      if not _verify_length(pword1, minLength=8, maxLength=32):
         raise cherrypy.HTTPError(400, 
                                'Password must be between 8 and 32 characters')
         
      checkUser = self.scribe.findUser(userId, email)
      
      if checkUser is None:
         usr = LMUser(userId, email, pword1, firstName=firstName, 
                      lastName=lastName, institution=institution, 
                      addr1=address1, addr2=address2, addr3=address3, 
                      phone=phone)
         insUsr = self.scribe.findOrInsertUser(usr)
         
         cherrypy.session[SESSION_KEY] = cherrypy.request.login = userId
         
         welcomeMsg = _get_welcome_msg(firstName, userId, pword1)
         return welcomeMsg
      else:
         raise cherrypy.HTTPError(409, 'Duplicate user credentials')
      
# .............................................................................
def _get_login_page():
   loginPage = """\
<html>
   <head>
      <title>Log in to Lifemapper</title>
   </head>
   <body>
      <div class="loginDiv" align="center">
         <form action="/api/login" method="post">
            <div align="center">
               <table>
                  <tr>
                     <td style="text-align: right;">
                        User Name: 
                     </td>
                     <td style="text-align: left;">
                        <input type="text" name="username" />
                     </td>
                  </tr>
                  <tr>
                     <td style="text-align: right;">
                        Password:
                     </td>
                     <td style="text-align: left;">
                        <input type="password" name="pword" />
                     </td>
                  </tr>
               </table>
            </div>
            <input type="submit" value="Log In" /><br /><br />
            New user? <a href="/api/signup">Sign up</a> here!<br /><br />
            Forgot your password? Contact us at lifemapper at ku dot edu.<br />
         </form>
      </div>
   </body>
</html>"""
   return loginPage

def _get_signup_page():
   signupPage = """\
<html>
   <head>
      <title>
         Sign up for Lifemapper
      </title>
   </head>
   <body>
<div align="center" class="signup">
   <form name="signup" action="/api/signup" method="post" 
         onsubmit="return validateNewUser(this);">
      <div align="center">
         <table>
            <tr>
               <td class="signupLabel">
                  User Id:
               </td>
               <td class="signupInput">
                  <input name="userId" id="userIdField" type="text" 
                                             onchange="checkUserName(this);" />
               </td>
               <td class="signupRequired">
                  (Required)
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Email Address:
               </td>
               <td class="signupInput">
                  <input name="email" id="emailField" type="text" 
                                                onchange="checkEmail(this);" />
               </td>
               <td class="signupRequired">
                  (Required)
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  First Name:
               </td>
               <td class="signupInput">
                  <input name="firstName" type="text" />
               </td>
               <td class="signupRequired">
                  (Required)
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Last Name:
               </td>
               <td class="signupInput">
                  <input name="lastName" type="text" />
               </td>
               <td class="signupRequired">
                  (Required)
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Institution:
               </td>
               <td class="signupInput">
                  <input name="institution" type="text" />
               </td>
               <td class="signupRequired">
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Address 1:
               </td>
               <td class="signupInput">
                  <input name="address1" type="text" />
               </td>
               <td class="signupRequired">
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Address 2:
               </td>
               <td class="signupInput">
                  <input name="address2" type="text" />
               </td>
               <td class="signupRequired">
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Address 3:
               </td>
               <td class="signupInput">
                  <input name="address3" type="text" />
               </td>
               <td class="signupRequired">
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Phone Number:
               </td>
               <td class="signupInput">
                  <input name="phone" type="text" />
               </td>
               <td class="signupRequired">
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Password:
               </td>
               <td class="signupInput">
                  <input name="pword1" type="password" />
               </td>
               <td class="signupRequired">
                  (Required)
               </td>
            </tr>
            <tr>
               <td class="signupLabel">
                  Confirm Password:
               </td>
               <td class="signupInput">
                  <input name="pword2" type="password" />
               </td>
               <td class="signupRequired">
                  (Required)
               </td>
            </tr>
         </table>
      </div>
      <br />
      <input name="tos" type="checkbox" /> 
      I have read and agree to the 
      <a href="http://lifemapper.org/?page_id=1096" target="_blank">terms of service</a>.<br />
      <br />
      <input type="submit" id="signUpButton" value="Sign Up!" /><br />
   </form>
</div>

<script type="text/javascript">
function validateNewUser(frm) {
   userId = document.getElementById('userIdField').value;
   email = document.getElementById('emailField').value;
   fName = document.getElementsByName('firstName')[0].value;
   lName = document.getElementsByName('lastName')[0].value;
   institution = document.getElementsByName('institution')[0].value;
   add1 = document.getElementsByName('address1')[0].value;
   add2 = document.getElementsByName('address2')[0].value;
   add3 = document.getElementsByName('address3')[0].value;
   phone = document.getElementsByName('phone')[0].value;
   pword1 = document.getElementsByName('pword1')[0].value;
   pword2 = document.getElementsByName('pword2')[0].value;
   
   if (userId.length > 0) {
      if (!validateLength(userId, 20)) {
         alert("User name must be 20 characters or less");
         return false;
      }
   } else {
      alert("User id is a required field");
      return false;
   }

   if (email.length < 1) {
      alert("Email is a required field");
      return false;
   }

   if (!validateEmail(email)) {
      alert("Invalid email address");
      return false;
   } else {
      if (!validateLength(email, 64)) {
         alert("Email must be 64 characters or less");
         return false;
      }
   }

   if (fName < 1) {
      alert("First name is a required field");
      return false;
   }

   if (!validateLength(fName, 50)) {
      alert("First name must be 50 characters or less");
      return false;
   }

   if (!validateLength(lName, 50)) {
      alert("Last name must be 50 characters or less");
      return false;
   }

   if (!validateLength(phone, 20)) {
      alert("Phone must be 20 characters or less");
      return false;
   }

   if (pword1.length < 1 || pword2.length < 1) {
      alert("A password is required");
      return false;
   }

   if (pword1 != pword2) {
      alert("Password does not match");
      return false;
   }

   if (!validateLength(pword1, 32)) {
      alert("Password must be 32 characters or less");
      return false;
   }

   return true;

}

function validateEmail(email) {
   var re = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
   return re.test(email);
}

function validateLength(val, maxLen) {
   if (val.length <= maxLen) {
      return true;
   } else {
      return false;
   }
}
   

function checkEmail(fld) {
   if (!validateEmail(fld.value)) {
      alert("Invalid email");
      document.getElementById('emailField').focus();
      return false;
   }
}

function checkUserName(fld) {
   if (!validateLength(fld.value, 20)) {
      alert("User name must be 20 characters or less");
      return false;
   }
}

</script>
   </body>
</html>"""
   return signupPage

def _get_welcome_msg(firstName, userId, pword):
   """
   @summary: Get a welcome message for the new user
   """
   welcomeMsg = """\
<html>
   <head>
      <title>
         Welcome to Lifemapper
      </title>
   </head>
   <body>
      <p>
         Your user name is: {userName}, your password is: {pword}
      </p>
   </body>
</html>""".format(userName=userId, pword=pword)
   return welcomeMsg

def _verify_length(item, minLength=0, maxLength=50):
   if item is None or (len(item) <= maxLength and len(item) >= minLength):
      return True
   else:
      return False
   