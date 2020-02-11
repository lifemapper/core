"""Module containing functions used to send emails from Lifemapper
"""
import smtplib

from LmServer.common.localconstants import SMTP_SERVER, SMTP_SENDER


# .............................................................................
class EmailNotifier(object):
    """
    @summary: Class used to connect to an SMTP server and send emails
    """

    # ....................................
    def __init__(self, server=SMTP_SERVER,
                             fromAddr=SMTP_SENDER):
        """
        @summary: Constructor
        @param server: (optional) SMTP server to send email from
        @param fromAddr: (optional) The email address to send emails from
        """
        self.fromAddr = fromAddr
        self.server = smtplib.SMTP(server)

    # ....................................
    def sendMessage(self, toAddrs, subject, msg):
        """
        @summary: Sends an email using the EmailNotifier's SMTP server to the 
                         specified recipients
        @param toAddrs: List of recipients
        @param subject: The subject of the email
        @param msg: The content of the email
        """
        if not isinstance(toAddrs, list):
            toAddrs = [toAddrs]

        mailMsg = ("From: {}\r\nTo: {}\r\nSubject: {}\r\n\r\n{}".format(
                                        self.fromAddr, ", ".join(toAddrs), subject, msg))
        try:
            self.server.sendmail(self.fromAddr, toAddrs, mailMsg)
        except Exception as e:
            # raise LMError(e.args)
            # This had to be changed because we don't want to put LMError on LmBackend
            raise e
