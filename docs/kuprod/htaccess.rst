# Our .htaccess file
```
RewriteEngine On
Options +FollowSymlinks

RewriteBase /
RewriteCond %{HTTP_HOST} ^dev.lifemapper.org	[OR]
RewriteCond %{HTTP_HOST} ^lifemapper.org	[OR]
RewriteCond %{HTTP_HOST} ^lifemapper.com


# Lifemapper service machine static content
RewriteCond %{REQUEST_URI} ^/(clients|css|dl|images|schemas|robots.txt)(.*)
RewriteRule  ^(clients|css|dl|images|schemas|robots.txt)(.*) http://svc.lifemapper.org/$1$2 [NC,R,L]

# Lifemapper service machine user services
RewriteCond %{REQUEST_URI} ^/(findUser|login|logout|signup)(.*)
RewriteRule  ^(findUser|login|logout|signup)(.*)  http://svc.lifemapper.org/$1$2  [NC,R,L]

# Lifemapper service machine Google links
RewriteCond %{REQUEST_URI} ^/(spLinks)(.*)
RewriteRule ^(spLinks)(.*)  http://svc.lifemapper.org/$1$2  [NC,R,L]

# Lifemapper service machine web services
RewriteCond %{REQUEST_URI} ^/(hint|ogc|services|jobs)(.*)
RewriteRule ^(hint|ogc|services|jobs)(.*)  http://svc.lifemapper.org/$1$2  [NC,R,L]

# Species Page Rewrites
RewriteCond %{REQUEST_URI} ^/species/(.+)
RewriteRule ^species/(.*)  /?page_id=863&speciesname=$1   [NC,R,L]

RewriteCond %{REQUEST_URI} ^/species
RewriteRule ^species /?page_id=863     [NC,R,L]


# IT put this in
RewriteCond %{REQUEST_URI} !blog/
RewriteRule ^(.*)$ blog/$1 [L]
```

# Breakdown
```
RewriteEngine On
Options +FollowSymlinks
```
Turn on the rewrite engine and set Apache to follow symbolic links

```
RewriteBase /
RewriteCond %{HTTP_HOST} ^dev.lifemapper.org	[OR]
RewriteCond %{HTTP_HOST} ^lifemapper.org	[OR]
RewriteCond %{HTTP_HOST} ^lifemapper.com
```
Set the base directory for rewrites and set the rewrite rules to work for any urls with host dev.lifemapper.org, lifemapper.org, or lifemapper.com

```
# Lifemapper service machine static content
RewriteCond %{REQUEST_URI} ^/(clients|css|dl|images|schemas|robots.txt)(.*)
RewriteRule  ^(clients|css|dl|images|schemas|robots.txt)(.*) http://svc.lifemapper.org/$1$2 [NC,R,L]
```
Set URLs that reference static content in the clients, css, dl, images, or schemas directories, or the robots.txt file to forward to http://svc.lifemapper.org

```
# Lifemapper service machine user services
RewriteCond %{REQUEST_URI} ^/(findUser|login|logout|signup)(.*)
RewriteRule  ^(findUser|login|logout|signup)(.*)  http://svc.lifemapper.org/$1$2  [NC,R,L]
```
Forward user related requests to svc.lifemapper.org

```
# Lifemapper service machine Google links
RewriteCond %{REQUEST_URI} ^/(spLinks)(.*)
RewriteRule ^(spLinks)(.*)  http://svc.lifemapper.org/$1$2  [NC,R,L]
```
Forward species link requests to svc.lifemapper.org

```
# Lifemapper service machine web services
RewriteCond %{REQUEST_URI} ^/(hint|ogc|services|jobs)(.*)
RewriteRule ^(hint|ogc|services|jobs)(.*)  http://svc.lifemapper.org/$1$2  [NC,R,L]
```
Forward web service URLs to svc.lifemapper.org

```
# Species Page Rewrites
RewriteCond %{REQUEST_URI} ^/species/(.+)
RewriteRule ^species/(.*)  /?page_id=863&speciesname=$1   [NC,R,L]

RewriteCond %{REQUEST_URI} ^/species
RewriteRule ^species /?page_id=863     [NC,R,L]
```
Rewrite species page URLs so that they point at the Lifemapper application species page in Wordpress

```
# IT put this in
RewriteCond %{REQUEST_URI} !blog/
RewriteRule ^(.*)$ blog/$1 [L]
```
This was added by the web folks.  I believe that it forwards requests to the Wordpress site which they have mounted in the blog directory. 


# Flags
From: https://httpd.apache.org/docs/2.4/rewrite/flags.html
 * L - If matches this pattern, this should be the last rule followed
 * NC - Case insensitive
 * R - Redirect