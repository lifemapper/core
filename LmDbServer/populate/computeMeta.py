"""
@summary: Metadata on LmCompute instances, and their primary contact,
          authorized to access this LmServer
@author: Aimee Stewart

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
# Required values are: Key: LmCompute name  
#                      Values: ip, contactid, email, password 
#                        encrypted defaults to False if not supplied
LM_COMPUTE_INSTANCES = {'BIRC': {
                           'ip': '129.237.201.247',
                           'contactid': 'aimee.stewart@ku.edu',
                           'email': 'aimee.stewart@ku.edu', 
                           'password': 'whatever',
                           'encrypted': False, 
                           'first': 'Aimee', 
                           'last':  'Stewart', 
                           'institution': 'KUBI',
                           'addr1': '1345 Jayhawk Blvd',
                           'addr2': 'Lawrence, KS  66045',
                           'addr3': 'USA'},
                        'JUNO': {
                           'ip': '129.237.201.230',
                           'contactid': 'aimee.stewart@ku.edu',
                           'email': 'aimee.stewart@ku.edu', 
                           'password': 'whatever',
                           'encrypted': False, 
                           'first': 'Aimee', 
                           'last':  'Stewart', 
                           'institution': 'KUBI',
                           'addr1': '1345 Jayhawk Blvd',
                           'addr2': 'Lawrence, KS  66045',
                           'addr3': 'USA'},
                        'FELIX': {
                           'ip': '129.237.201.230',
                           'contactid': 'aimee.stewart@ku.edu',
                           'email': 'aimee.stewart@ku.edu', 
                           'password': 'whatever',
                           'encrypted': False, 
                           'first': 'Aimee', 
                           'last':  'Stewart', 
                           'institution': 'KUBI',
                           'addr1': '1345 Jayhawk Blvd',
                           'addr2': 'Lawrence, KS  66045',
                           'addr3': 'USA'},
                        'BADENOV': {
                           'ip': '129.237.201.119',
                           'contactid': 'aimee.stewart@ku.edu',
                           'email': 'aimee.stewart@ku.edu', 
                           'password': 'whatever',
                           'encrypted': False, 
                           'first': 'Aimee', 
                           'last':  'Stewart', 
                           'institution': 'KUBI',
                           'addr1': '1345 Jayhawk Blvd',
                           'addr2': 'Lawrence, KS  66045',
                           'addr3': 'USA'},
                        'SPORKS': {
                           'ip': '129.237.201.67',
                           'contactid': 'cjgrady@ku.edu',
                           'email': 'cjgrady@ku.edu', 
                           'password': 'whatever',
                           'encrypted': False, 
                           'first': 'CJ', 
                           'last':  'Grady', 
                           'institution': 'KUBI',
                           'addr1': '1345 Jayhawk Blvd',
                           'addr2': 'Lawrence, KS  66045',
                           'addr3': 'USA'}

                        }
