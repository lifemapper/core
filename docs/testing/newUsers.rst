****************
New User Testing
****************

   The current testing framework is not able to clean up new user requests.  Therefore, these tests need to be done manually for now.  For all tests, go to the user signup page at http://yourserver/signup.  If you are not presented with a new user sign up form, you likely need to log out of a the current user session.  Do that by visiting http://yourserver/logout

---------

==========
Basic test
==========
   The first test should be that a new user is created from valid inputs.  
   
   1. Fill in each entry of the form with valid data
   2. Check the box saying that you accept the Lifemapper terms of service
   3. Click the "Sign Up!" button
   4. You will be presented with a screen with your log in credentials and information
   5. Logout 

=============
Conflict Test
=============
   Test that duplicate user credentials throws an HTTP 409 (Conflict) error.  Do this by signing up for a new user account with the same user name as an existing account.

   1. Follow the instructions for the basic test
   2. Repeat the basic test instructions, using the same user name for the second user account
   3. After clicking the "Sign Up!" button, an HTTP 409 (Conflict) error should be thrown
 
===================
Missing fields test
===================
   Test that signing up for a new user account without all of the required fields throws an error.  If you do this outside of the web page, an HTTP 400 (Bad Request) error will be thrown.  The web page shouldn't let you submit the form.

   1. Fill in the form but omit one or more of the required fields
   2. Check the box saying that you accept the Lifemapper terms of service
   3. Click the "Sign Up!" button
   4. The page should show a message that one of the required fields is missing

==============
Invalid fields
==============
   Test that entering invalid information into one or more of the fields will throw an error.  If you do this from outside of the web page, an HTTP 400 (Bad Request) error will be thrown.  The web page shouldn't let you submit the form with invalid data.

   1. Options for invalid fields

      * An invalid email address
      * A user name that is too many characters
      * Other fields with too many characters

   2. Check the box saying that you accept the Lifemapper terms of service
   3. Click the "Sign Up!" button
   4. The page should show a message that one of the fields is invalid

=============
Unicode tests
=============
   Test that entering unicode values for fields does not throw an error.

   1. Fill in field values like first and last name, as well as address with unicode characters
   2. Check the box saying that you accept the Lifemapper terms of service
   3. Click the "Sign Up!" button
   4. A new user account should be created without problems
   5. Log out
   