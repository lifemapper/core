Start instructions:  http://cloudmesh.github.io/client/quickstart.html

Preparation:
------------

In terminal:
   * Install xcode::
       xcode-select --install
   * Download python 2.7.12 at https://www.python.org/downloads/
   * Run script  /Applications/Python2.7/Update Shell Profile.command
   * Open new terminal::
    
     $ python --version
     $ pip --version
     $ sudo easy_install pip
     $ sudo -H pip install --upgrade pip
     $ sudo -H pip install virtualenv
         
   * Check versions - should be python 2.7.12, pip 8.1.2, virtualenv 15.0.1::
     $ python --version
     $ pip --version
     $ virtualenv --version
         
   * Create virtual environment::
     $ virtualenv -p /Library/Frameworks/Python.framework/Versions/2.7/bin/python ~/ENV

   * Activate virtual environment (add to .bash_profile if desired)::
     $ source ~/ENV/bin/activate
     (ENV)$ export PYTHONPATH=~/ENV/lib/python2.7/site-packages:$PYTHONPATH

   * Upgrade virtual environment::
     (ENV)$ pip install pip -U
     (ENV)$ easy_install readline
     (ENV)$ easy_install pycrypto
     (ENV)$ pip install urllib3

   * Double check versions::
     (ENV)$ python --version
     (ENV)$ pip --version

Installation:
-------------

   * Install cloudmesh (in the virtual environment) from source::

     (ENV)$ git clone https://github.com/cloudmesh/client.git
     (ENV)$ cd client
     (ENV)$ pip install -r requirements.txt
     (ENV)$ pip install -U .

   * Configure cloudmesh with ssh key??::
    
     (ENV)$ cm key add --ssh ~/.ssh/id_rsa.pub

   * Configure cloudmesh by calling command to create ~/.cloudmesh/cloudmesh.yaml file

     (ENV)$ cm help 

   * Get user/password from admin, then initialize with ::
     (ENV)$ cm comet init

   * Change password

Using:
------

   * Follow instructions at http://cloudmesh.github.io/client/commands/command_comet.html

   * List clusters::
   
(ENV) rocky-2:client astewart$ cm comet ll
+------+---------+-------+--------------+---------------+------------+-----------+-------------+
| Name | Project | Count | Nodes        | Frontend (Fe) | State (Fe) | Type (Fe) | Description |
+------+---------+-------+--------------+---------------+------------+-----------+-------------+
| vc6  | kan114  | 8     | vm-vc6-[0-7] | vc6           | nostate    | VM        |             |
+------+---------+-------+--------------+---------------+------------+-----------+-------------+

(ENV) rocky-2:client astewart$ cm comet cluster vc6
Cluster: vc6   Frontend: vc6  IP: 198.202.74.181
+----------+---------+----------+------+-------------------+----------------+------+---------+--------+---------+------------+------------+-------------+
| name     | state   | kind     | type | mac               | ip             | cpus | cluster | RAM(M) | disk(G) | computeset | allocation | admin_state |
+----------+---------+----------+------+-------------------+----------------+------+---------+--------+---------+------------+------------+-------------+
| vc6      | nostate | frontend | VM   | ca:5f:a8:00:00:72 | 198.202.74.181 | 2    | vc6     | 4096   | 36      |            |            | ready       |
|          |         |          |      | ca:5f:a8:00:00:73 |                |      |         |        |         |            |            |             |
| vm-vc6-0 | active  | compute  | VM   | ca:5f:a8:00:00:74 |                | 24   | vc6     | 120000 | 36      | 2909       | kan114     | synced      |
| vm-vc6-1 | active  | compute  | VM   | ca:5f:a8:00:00:75 |                | 24   | vc6     | 120000 | 36      | 2909       | kan114     | synced      |
| vm-vc6-2 | active  | compute  | VM   | ca:5f:a8:00:00:76 |                | 24   | vc6     | 120000 | 36      | 2909       | kan114     | synced      |
| vm-vc6-3 | active  | compute  | VM   | ca:5f:a8:00:00:77 |                | 24   | vc6     | 120000 | 36      | 2909       | kan114     | synced      |
| vm-vc6-4 | active  | compute  | VM   | ca:5f:a8:00:00:78 |                | 24   | vc6     | 120000 | 36      | 2909       | kan114     | synced      |
| vm-vc6-5 | active  | compute  | VM   | ca:5f:a8:00:00:79 |                | 24   | vc6     | 120000 | 36      | 2909       | kan114     | synced      |
| vm-vc6-6 | active  | compute  | VM   | ca:5f:a8:00:00:7a |                | 24   | vc6     | 120000 | 36      | 2909       | kan114     | synced      |
| vm-vc6-7 | active  | compute  | VM   | ca:5f:a8:00:00:7b |                | 24   | vc6     | 120000 | 36      | 2909       | kan114     | synced      |
+----------+---------+----------+------+-------------------+----------------+------+---------+--------+---------+------------+------------+-------------+
(ENV)
    
   * List ISOs, then attach kernel iso to frontend::

     (ENV) rocky-2:client astewart$ cm comet iso list
     ...
     3: kernel-6.2-0.x86_64.disk1.iso
     ...
     (ENV) rocky-2:client astewart$ cm comet iso attach 3 vc6

   * Power off/on frontend, then reset (to catch Rocks splash screen)::
   
     (ENV)$ cm comet power off vc6 
     (ENV)$ cm comet power on vc6
     (ENV)$ cm comet power reset vc6
     
   * Wait a few minutes, then open a console::

     (ENV)$ cm comet console vc6

   * install Rocks on frontend::
      
      build ip=198.202.74.181 gateway=198.202.74.1 dns=8.8.8.8 ksdevice=eth1 netmask=255.255.255.0
  
   * hostname = hpcdev-pub11.sdsc.edu
   
   * Add nodes to frontend
     * ssh into frontend
       $ insert-ethers
     * choose 'Compute' appliance
     * power on node
       (ENV)$ cm comet power off vc6 vm-vc6-0
   
   
   * insert-etherws