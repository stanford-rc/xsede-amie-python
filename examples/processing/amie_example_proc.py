#!/usr/bin/python
#
# 2016 - Stephane Thiell <sthiell@stanford.edu>

"""
Example of AMIE packet handling implementation using the Pyamie library.

This script processes pending incoming AMIE transactions and calls
handler methods from the class ExampleSitePacketHandler.

This example is based on AMIE implementation developed for Stanford's
XStream GPU cluster, featuring:
- SLURM accounting management
- Custom cluster user account management scripts
- Slack notifications

This script cannot be used "as is", this is just an EXAMPLE of how to
use the Pyamie library.
"""

import argparse
from datetime import datetime
import json
import logging
import pwd
import requests
import yaml

from ClusterShell.Task import task_self

from pyamie.packet import PacketHandler, PacketIgnoredException
from pyamie.task import AMIETask

LOGGER = logging.getLogger(__name__)


class ExampleSitePacketHandler(PacketHandler):

    def __init__(self, settings):
        PacketHandler.__init__(self)
        self.settings = settings
        self.task = task_self()
        self.task.set_info("ssh_user", settings['mgmt_user'])

    def slack_amie_packet_notification(self, packet, data_in, data_keys, emoji):
        """Create and send a Slack notification from a AMIE packet"""

        if 'slack_channel' not in self.settings:
            return

        fields = []
        for key in data_keys:
            if data_in.getone(key):
                fields.append({"title": key,
                               "value": data_in[key],
                               "short": "true"})

        text = "%s *%s* _trans %s, packet %s_" % (emoji, packet.type_name,
                                                  packet.trans.trans_rec_id,
                                                  packet.packet_rec_id)
        fallback = packet.type_name

        payload = {"text": text,
                   "fallback": fallback,
                   "channel": self.settings['slack_channel'],
                   "username": self.settings['slack_bot_username'],
                   "attachments": [{"fallback": fallback,
                                    "fields": fields}]}

        res = requests.post(self.settings['slack_webhook_url'], data=json.dumps(payload))
        LOGGER.info("slack webhook returned %s", res)

    def slack_msg_notification(self, msg, emoji):
        """Send a Slack message notification"""

        if 'slack_channel' not in self.settings:
            return

        text = "%s %s" % (emoji, msg)

        payload = {"text": text,
                   "fallback": text,
                   "channel": self.settings['slack_channel'],
                   "username": self.settings['slack_bot_username']}

        res = requests.post(self.settings['slack_webhook_url'], data=json.dumps(payload))
        LOGGER.info("slack webhook returned %s", res)

    def _exec_mgmt(self, cmd):
        """Execute a command on the cluster head node"""
        LOGGER.info("mgmt exec: %s", cmd)
        self.task.run(nodes=self.settings['mgmt_host'], command=cmd)
        LOGGER.info("%s", str(self.task.node_buffer(self.settings['mgmt_host'])))
        return self.task.max_retcode()

    def _exec_local(self, cmd):
        """Execute a local command"""
        LOGGER.info("local exec: %s", cmd)
        self.task.run(command=cmd, key='local')
        LOGGER.info("%s", str(self.task.node_buffer('local')))
        return self.task.max_retcode()

    def _add_user(self, data_in, group_name, is_pi=False):
        """Helper to add an user"""

        if is_pi:
            key_suffix = 'Pi'
        else:
            key_suffix = 'User'

        global_id_key = key_suffix + 'GlobalID'
        person_id_key = key_suffix + 'PersonID'
        email_key = key_suffix + 'Email'
        firstname_key = key_suffix + 'FirstName'
        middlename_key = key_suffix + 'MiddleName'
        lastname_key = key_suffix + 'LastName'
        reqloginlist_key = key_suffix + 'RequestedLoginList'

        login = None

        if data_in.getone(person_id_key):
            # PI is already known
            person_id = int(data_in[person_id_key])
            LOGGER.warning("User %s is already known by TGCDB", person_id)
            login = pwd.getpwuid(person_id).pw_name # may raise KeyError if not found
            LOGGER.warning("Found login %s for known user %s", login, person_id)

            self.slack_msg_notification("Found existing login *%s* for known PersonID %s"
                                        % (login, person_id),
                                        ":information_source:")
        if not login:
            # Check for GlobalID
            if data_in.getone(global_id_key):
                globalid = data_in[global_id_key]
                #
                # NOTE: You need to implement a way to retrieve the PersonID from GlobalID!
                # We use LDAP for that at Stanford, but you could use a DB.
                #
                #person_id = ldap_find_globalid(self.settings, globalid)
                #if person_id:
                #    LOGGER.warning("%s %s is already known locally (%d)",
                #                   global_id_key, globalid, person_id)
                #    login = pwd.getpwuid(person_id).pw_name # may raise KeyError if not found
                #    LOGGER.warning("Found login %s for known user %s", login, person_id)
                #
                #    self.slack_msg_notification("Found existing login *%s* (%d) for known GlobalID %s"
                #                                % (login, person_id, globalid),
                #                                ":information_source:")
                raise NotImplementedError('You need to implement GlobalID to PersonID mapping!')

        if login:
            cmd = '%s --mod-user %s -G %s' % (self.settings['mgmt_cmd'], login,
                                              group_name)
            if self._exec_mgmt(cmd) != 0:
                raise ValueError("failure when adding supplementary group %s "
                                 "for user %s" % (group_name, login))

            LOGGER.info("successfully added user %s to supplementary group %s",
                        login, group_name)

        else:
            # New PI/User
            firstname = data_in[firstname_key]
            lastname = data_in[lastname_key]

            email = data_in.getone(email_key)

            if not email:
                LOGGER.warning("Missing %s for %s %s", email_key, firstname, lastname)
                email = "xs-dummy@localhost" # required by XStream LDAP schema

            # Build GEICO using first + optional middle + last names
            if data_in.getone(middlename_key):
                geico = ' '.join((firstname, data_in[middlename_key], lastname))
            else:
                geico = ' '.join((firstname, lastname))

            login_candidates = data_in.get(reqloginlist_key, None)
            LOGGER.info("login_candidates %s: %s", reqloginlist_key, login_candidates)
            if login_candidates:
                for i, login_candidate in enumerate(login_candidates):
                    if not login_candidate:
                        continue
                    try_login = "xs-%s" % login_candidate
                    # NOTE: this is very specific to XStream and cannot be reused as is.
                    # We store the XDCDB GlobalID in the LDAP description.
                    desc = "XDCDBGlobalID=%s" % data_in.getone(global_id_key)
                    cmd = '%s --add-user %s --group %s --gecos "%s" --mail "%s" --desc "%s"' % \
                          (self.settings['mgmt_cmd'], try_login, group_name, geico, email, desc)
                    if self._exec_mgmt(cmd) == 0:
                        login = try_login
                        break

            if not login:
                LOGGER.warning("login not found by ReqLoginList for %s %s",
                               firstname, lastname)
                login_candidates = data_in.get('SitePersonId', 'PersonID')
                login_sites = data_in.get('SitePersonId', 'Site')
                for siteref in ('XD-ALLOCATIONS', 'XD-PORTAL'):
                    LOGGER.info("trying to determine login by site: %s", siteref)
                    try:
                        idx = login_sites.index(siteref)
                        try_login = "xs-%s" % login_candidates[idx]
                        # NOTE: this is very specific to XStream and cannot be reused as is.
                        # We store the XDCDB GlobalID in the LDAP description.
                        desc = "XDCDBGlobalID=%s" % data_in.getone(global_id_key)
                        cmd = '%s --add-user %s --group %s --gecos "%s" --mail "%s" --desc "%s"' % \
                              (self.settings['mgmt_cmd'], try_login, group_name, geico, email, desc)
                        if self._exec_mgmt(cmd) == 0:
                            login = try_login
                            break
                    except ValueError:
                        pass

        if not login:
            raise ValueError("Cannot create user from ```%s```" % str(data_in))

        pw = pwd.getpwnam(login)
        person_id = pw.pw_uid

        LOGGER.info("user account: %s uid %s", login, person_id)

        # add user to project in SLURM
        cmd = "sacctmgr -i add user %s account=%s %s" % (login, group_name,
                                                         self.settings['sacctmgr_user_options'])
        rc = self._exec_local(cmd)
        LOGGER.info("sacctmgr returned %s", rc)
        if rc != 0:
            raise ValueError("sacctmgr: failed to create SLURM account for "
                             "user %s" % login)

        if is_pi:
            # add PI as account coordinator
            cmd = "sacctmgr -i add coordinator account=%s names=%s" % (group_name, login)
            rc = self._exec_local(cmd)
            LOGGER.info("sacctmgr returned %s", rc)

            if rc == 0:
                self.slack_msg_notification("SLURM: activated account coordinator *%s* (%s)"
                                            % (login, group_name),
                                            ":white_check_mark:")
            else:
                self.slack_msg_notification("SLURM: failed to activate account "
                                            "coordinator *%s* (%s) err=%s"
                                            % (login, group_name, rc),
                                            ":exclamation:")

        else:
            self.slack_msg_notification("SLURM: activated user *%s* in %s"
                                        % (login, group_name),
                                        ":white_check_mark:")

        return (pw.pw_name, person_id)

    def _add_dn_entry(self, data_in):
        """Helper to add a user DNs"""
        pw = pwd.getpwuid(int(data_in['PersonID']))

        # Authorize known user DNs
        dnlist = data_in.get('DnList')
        LOGGER.info("_add_dn_entry: known person DNs are %s", dnlist)

        grid_mapfile = self.settings['grid_mapfile']

        for dn in dnlist:
            cmd = 'grid-mapfile-add-entry -dn "%s" -ln %s -f "%s"' \
                  % (dn, pw.pw_name, grid_mapfile)
            rc = self._exec_local(cmd)
            if rc:
                raise ValueError('grid-mapfile-add-entry failed for user %s '
                                 'DN "%s" (mapfile=%s rc=%s)'
                                 % (pw.pw_name, dn, grid_mapfile, rc))

        self.slack_msg_notification("GSI-SSH: successfully added %d DNs for "
                                    "user *%s*" % (len(dnlist), pw.pw_name),
                                    ":white_check_mark:")

    def _delete_dn_entry(self, data_in):
        """Helper to delete a user DNs"""
        pw = pwd.getpwuid(int(data_in['PersonID']))

        dnlist = data_in.get('DnList')
        LOGGER.info("_delete_dn_entry: person DNs to delete are %s", dnlist)

        grid_mapfile = self.settings['grid_mapfile']

        success = 0
        for dn in dnlist:
            cmd = 'grid-mapfile-delete-entry -dn "%s" -ln %s -f "%s"' \
                   % (dn, pw.pw_name, grid_mapfile)
            rc = self._exec_local(cmd)
            if rc:
                msgfmt = 'GSI-SSH: grid-mapfile-delete-entry failed for user ' \
                         '%s DN "%s" (mapfile=%s rc=%s)'
                self.slack_msg_notification(msgfmt % (pw.pw_name, dn,
                                                      grid_mapfile, rc),
                                            ":exclamation:")
            else:
                success += 1

        if success > 0:
            self.slack_msg_notification("GSI-SSH: successfully deleted %d DNs "
                                        "for user *%s*" % (success, pw.pw_name),
                                        ":white_check_mark:")

    def request_project_create(self, packet, data_in, data_out):
        # request_project_create: mandatory fields:
        # - AllocationType
        # - EndDate
        # - GrantNumber
        # - PfosNumber
        # - PiFirstName
        # - PiLastName
        # - PiOrganization
        # - PiOrgCode
        # - ResourceList
        # - ServiceUnitsAllocated
        # - StartDate

        # May be known:
        # - ProjectID
        # - ProjectTitle
        # - PiDnList

        keys = ['AllocationType', 'GrantNumber', 'PfosNumber', 'PiFirstName',
                'PiLastName', 'PiOrganization', 'PiOrgCode', 'ResourceList',
                'ServiceUnitsAllocated', 'StartDate', 'EndDate']
        try:
            project_title = data_in['ProjectTitle'].replace('"', '\\"').lower()
            keys.append('ProjectTitle')
        except KeyError:
            project_title = None

        self.slack_amie_packet_notification(packet, data_in, keys,
                                            ":sports_medal:")

        # Generate ProjectID
        project_id = "P-%s" % data_in['GrantNumber']
        group_name = project_id.lower()

        in_project_id = data_in.get('ProjectID')
        if in_project_id:
            LOGGER.info("request_project_create: ProjectID=%s project_id=%s",
                        in_project_id, project_id)
            assert in_project_id[0] == project_id

        # SU
        project_su = int(data_in['ServiceUnitsAllocated'])
        alloc_type = data_in['AllocationType']

        if in_project_id and alloc_type == 'extension':
            self.slack_msg_notification("Received *extension* for Grant %s"
                                        % data_in['GrantNumber'],
                                        ":date:")
            uid = data_in['PiPersonID']
            login = pwd.getpwuid(int(uid)).pw_name


        elif in_project_id and alloc_type in ('transfer', 'supplement',
                                              'advance', 'renewal'):
            self.slack_msg_notification("Received *%s* for Grant %s"
                                        % (alloc_type, data_in['GrantNumber']),
                                        ":fuelpump:")
            uid = data_in['PiPersonID']
            login = pwd.getpwuid(int(uid)).pw_name

            #
            # NOTE: here you need to implement a way to retrieve current project SUs
            #

            ## Get current project SUs
            #task = task_self()
            #cmd = 'sacctmgr -p list assoc tree account=%s format=Account,GrpTRESMins' % group_name
            #task.run(command=cmd, key='sacctmgr')
            #output = str(task.key_buffer('sacctmgr'))
            #LOGGER.info("sacctmgr: %s", output)
            #LOGGER.info("sacctmgr retcode = %s", task.max_retcode())
            #if task.max_retcode() != 0:
            #    raise ValueError("sacctmgr failed with error %s" % task.max_retcode())
            #
            #gpu_mins = 0
            #for line in output.splitlines():
            #    if line.startswith('p-'):
            #        project, gres = line.split('|', 1)
            #        gpu_mins = int(re.search(r'gpu=(\d+)', gres).group(1))
            #        break
            #
            #if not gpu_mins:
            #    raise ValueError("sacctmgr failed to get GrpTRESMins for account %s" % group_name)

            ## get additional SUs
            #assert project_su > 0 # only positive transfer supported for now
            ## add already allocated SUs
            #project_su += gpu_mins / 60
            ## recompute allocation (with XStream gpu/cpu ratio)
            #gpu_mins = 60 * project_su
            #cpu_mins = (20 * gpu_mins) / 16

            #cmd = 'sacctmgr -i update account %s set ' \
            #      'GrpTRESMins=cpu=%d,gres/gpu=%d' % (group_name, cpu_mins,
            #                                          gpu_mins)
            #task.run(command=cmd, key='sacctmgr')
            #LOGGER.info("%s", str(task.key_buffer('sacctmgr')))
            #LOGGER.info("sacctmgr retcode = %s", task.max_retcode())
            #if task.max_retcode() != 0:
            #    raise ValueError("sacctmgr failed with error %s" % task.max_retcode())
            #
            #slack_msg = "ServiceUnitsAllocated for Grant %s (account %s) successfully to %d"
            #self.slack_msg_notification(slack_msg % (data_in['GrantNumber'],
            #                                         group_name, project_su),
            #                            ":fuelpump:")
            raise NotImplementedError('You need to implement project transfer/suppl/advance')
        else:
            # Create project group
            cmd = "%s --add-group %s --container xsede" % (self.settings['mgmt_cmd'],
                                                           group_name)
            if project_title:
                cmd += ' --desc "%s"' % project_title

            rc = self._exec_mgmt(cmd)
            LOGGER.info("create group project returned %s", rc)
            if rc:
                raise ValueError("Failed to create group project (rc=%s)" % rc)

            login, uid = self._add_user(data_in, group_name, is_pi=True)

        # notify_project_create: mandatory fields
        # - AccountActivityTime
        # - BoardType
        # - GrantNumber
        # - PfosNumber
        # - PiOrgCode
        # - PiPersonID
        # - PiRemoteSiteLogin
        # - ProjectID
        # - ProjectTitle
        # - ResourceList
        # - ServiceUnitsAllocated
        # - StartDate
        data_out['AccountActivityTime'] = datetime.now().isoformat()
        #data_out['BoardType'] = 'NRAC'
        data_out['GrantNumber'] = data_in['GrantNumber']
        data_out['PfosNumber'] = data_in['PfosNumber']
        data_out['PiOrgCode'] = data_in['PiOrgCode']
        data_out['PiPersonID'] = uid
        data_out['PiRemoteSiteLogin'] = login
        data_out['ProjectID'] = project_id
        data_out['ProjectTitle'] = data_in['ProjectTitle']
        data_out['ResourceList'] = self.settings['resource']
        data_out['ServiceUnitsAllocated'] = "%d" % project_su
        data_out['StartDate'] = data_in['StartDate']

        for val in data_in.get('PiDnList'):
            data_out.append_record('PiDnList', None, val)

    def data_project_create(self, packet, data_in, data_out):
        # data_project_create: mandatory fields:
        # - PersonID
        # - ProjectID

        self._add_dn_entry(data_in)

        # The final packet in a transaction is an inform_transaction_complete
        # packet. This packet has two states: success and failure. A failure
        # may be sent in response to any packet. A success may only be sent in
        # reply to a packet that expects an inform_transaction_complete reply.

        # inform_transaction_complete
        # - DetailCode 1 = success, 2 = failure (not used)
        # - Message = message
        # - StatusCode = "Success" or "Failure"
        data_out['DetailCode'] = "1"
        data_out['Message'] = "Project successfully created"
        data_out['StatusCode'] = "Success"

    def request_account_create(self, packet, data_in, data_out):
        # request_account_create: mandatory fields
        # - GrantNumber
        # - ResourceList
        # - UserFirstName
        # - UserLastName
        # - UserOrganization
        # - UserOrgCode

        # Optional fields:
        # - UserRequestedLoginList
        # - UserEmail
        # - ProjectID may not be present, we always use GrantNumber instead
        keys = ['GrantNumber', 'ResourceList', 'UserFirstName', 'UserLastName',
                'UserOrganization', 'UserOrgCode', 'UserEmail']
        self.slack_amie_packet_notification(packet, data_in, keys, ":bow:")

        task = task_self()
        task.set_info("ssh_user", self.settings['mgmt_user'])

        # Resolve project group name
        project_id = "P-%s" % data_in['GrantNumber']
        group_name = project_id.lower()

        login, uid = self._add_user(data_in, group_name)

        # notify_account_create:
        # - ProjectID
        # - ResourceList
        # - UserOrganization
        # - UserOrgCode
        # - UserPersonID
        # - UserRemoteSiteLogin
        data_out['AccountActivityTime'] = datetime.now().isoformat()
        data_out['ProjectID'] = project_id
        data_out['ResourceList'] = self.settings['resource']
        data_out['UserOrgCode'] = data_in['UserOrgCode']
        data_out['UserOrganization'] = data_in['UserOrganization']
        data_out['UserFirstName'] = data_in['UserFirstName']
        data_out['UserLastName'] = data_in['UserLastName']
        if data_in.getone('UserMiddleName'):
            data_out['UserMiddleName'] = data_in['UserMiddleName']

        # local ID for the account user
        data_out['UserPersonID'] = uid

        # login at the local site for the user
        data_out['UserRemoteSiteLogin'] = login

        for val in data_in.get('UserDnList'):
            data_out.append_record('UserDnList', None, val)

    def data_account_create(self, packet, data_in, data_out):
        # Authorize known user DNs
        self._add_dn_entry(data_in)

        # And reply Success
        data_out['DetailCode'] = "1"
        data_out['Message'] = "User account successfully created"
        data_out['StatusCode'] = "Success"

    def request_project_inactivate(self, packet, data_in, data_out):
        # request_project_inactivate: mandatory fields
        # - ProjectID
        # - ResourceList
        # optional:
        # - Comment
        # - GrantNumber
        # - StartDate
        # - EndDate
        # - ServiceUnitsAllocated
        # - ServiceUnitsRemaining

        keys = ['ProjectID', 'ResourceList', 'ServiceUnitsAllocated',
                'ServiceUnitsRemaining']
        self.slack_amie_packet_notification(packet, data_in, keys,
                                            ":no_entry_sign:")

        task = task_self()
        group_name = data_in['ProjectID'].lower()

        # Inactivate this account in SLURM
        cmd = 'sacctmgr -i update account %s set GrpSubmitJobs=0' % group_name
        task.run(command=cmd, key='sacctmgr')
        LOGGER.info("%s", str(task.key_buffer('sacctmgr')))
        LOGGER.info("sacctmgr retcode = %s", task.max_retcode())
        if task.max_retcode() != 0:
            raise ValueError("sacctmgr failed with error %s" % task.max_retcode())

        self.slack_msg_notification("SLURM: deactivated account *%s*" % group_name,
                                    ":white_check_mark:")

        # response: notify_project_inactivate
        data_out['ProjectID'] = data_in['ProjectID']
        data_out['ResourceList'] = self.settings['resource']

    def request_project_reactivate(self, packet, data_in, data_out):
        # request_project_reactivate: mandatory fields
        # - ProjectID
        # - ResourceList
        # optional:
        # - Comment
        # - GrantNumber
        # - StartDate
        # - EndDate
        # - ServiceUnitsAllocated
        # - ServiceUnitsRemaining

        keys = ['ProjectID', 'ResourceList', 'ServiceUnitsAllocated', 'ServiceUnitsRemaining']
        self.slack_amie_packet_notification(packet, data_in, keys, ":battery:")

        task = task_self()
        group_name = data_in['ProjectID'].lower()

        # GrpSubmitJobs to -1 will re-activate this account in SLURM
        cmd = 'sacctmgr -i update account %s set GrpSubmitJobs=-1' % group_name
        rc = self._exec_local(cmd)
        LOGGER.info("sacctmgr returned %s", rc)
        if rc != 0:
            raise ValueError("request_project_reactivate: sacctmgr failed "
                             "with error %s (%s)" % (rc, group_name))

        # response: notify_project_inactivate
        data_out['ProjectID'] = data_in['ProjectID']
        data_out['ResourceList'] = self.settings['resource']

    def request_account_inactivate(self, packet, data_in, data_out):
        # request_account_inactivate: mandatory fields
        # - PersonID
        # - ProjectID
        # - ResourceList
        # optional:
        # - Comment

        task = task_self()
        group_name = data_in['ProjectID'].lower()

        pw = pwd.getpwuid(int(data_in['PersonID']))

        keys = ['PersonID', 'ProjectID', 'ResourceList', 'Comment',
                '(User Login)', '(Account)']

        slackdata = data_in.copy()
        slackdata['(User Login)'] = pw.pw_name
        slackdata['(Account)'] = group_name
        self.slack_amie_packet_notification(packet, slackdata, keys,
                                            ":no_pedestrians:")

        # Remove user from project
        cmd = 'sacctmgr -i remove user %s account=%s' % (pw.pw_name, group_name)
        rc = self._exec_local(cmd)
        LOGGER.info("sacctmgr retcoded %s", rc)
        if rc:
            raise ValueError("request_account_inactivate: sacctmgr failed with"
                             " error %s (%s, %s)" % (rc, pw.pw_name, group_name))

        self.slack_msg_notification("SLURM: deactivated user *%s* (%s)"
                                    % (pw.pw_name, group_name),
                                    ":white_check_mark:")
        # response: notify_project_inactivate
        data_out['PersonID'] = data_in['PersonID']
        data_out['ProjectID'] = data_in['ProjectID']
        data_out['ResourceList'] = self.settings['resource']

    def request_account_reactivate(self, packet, data_in, data_out):

        # /!\ Not used: AMIE uses request_account_create instead

        # request_project_reactivate: mandatory fields
        # - PersonID
        # - ProjectID
        # - ResourceList
        # optional:
        # - Comment

        task = task_self()
        group_name = data_in['ProjectID'].lower()

        pw = pwd.getpwuid(int(data_in['PersonID']))

        keys = ['PersonID', 'ProjectID', 'ResourceList', 'Comment',
                '(User Login)', '(Account)']

        slackdata = data_in.copy()
        slackdata['(User Login)'] = pw.pw_name
        slackdata['(Account)'] = group_name
        self.slack_amie_packet_notification(packet, slackdata, keys, ":zap:")

        # Remove user from project
        cmd = 'sacctmgr -i remove user %s account=%s' % (pw.pw_name, group_name)
        rc = self._exec_local(cmd)
        LOGGER.info("sacctmgr retcoded %s", rc)
        if rc:
            raise ValueError("request_account_reactivate: sacctmgr failed "
                             "with error %s (%s, %s)" % (rc, pw.pw_name,
                                                         group_name))

        # response: notify_project_inactivate
        data_out['PersonID'] = data_in['PersonID']
        data_out['ProjectID'] = data_in['ProjectID']
        data_out['ResourceList'] = self.settings['resource']

    def request_user_create(self, packet, data_in, data_out):
        # Not implemented
        raise PacketIgnoredException()

    def request_user_modify(self, packet, data_in, data_out):
        # request_user_modify: mandatory fields:
        # - ActionType
        # - PersonID
        keys = ['ActionType', 'PersonID']
        self.slack_amie_packet_notification(packet, data_in, keys,
                                            ":writing_hand:")

        if data_in['ActionType'] in ('add', 'replace'):
            # From AMIE-XSEDE about 'replace': if the DnList tag is present,
            # DNs listed must be added to the grid mapfile; DNs in the grid
            # mapfile which are not listed must be preserved. If the DnList
            # tag is not present, all DNs in the grid mapfile must be preserved.
            self._add_dn_entry(data_in)

        elif data_in['ActionType'] == 'delete':
            self._delete_dn_entry(data_in)

        # And reply Success
        data_out['DetailCode'] = "1"
        data_out['Message'] = "User account successfully updated"
        data_out['StatusCode'] = "Success"

def main():
    """main poller function: parse command line argument and poll AMIE DB"""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--config-file", required=True)
    parser.add_argument("-o", "--output-file")
    pargs = parser.parse_args()
    with open(pargs.config_file) as yamlfile:
        settings = yaml.load(yamlfile)

    logging.basicConfig(filename=pargs.output_file, level=logging.INFO,
                        format='%(asctime)s %(name)s %(levelname)s %(message)s')
    try:
        handler = ExampleSitePacketHandler(settings)
        AMIETask(settings['dsn'], handler, settings['timeout']).run()
    except Exception, exc:
        handler.slack_msg_notification("AMIE Processing Error: %s" % exc, ":x:")
        raise


if __name__ == '__main__':
    main()
