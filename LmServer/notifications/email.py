"""Module containing functions used to send emails from Lifemapper
"""
import smtplib

from LmBackend.common.lmobj import LMError
from LmServer.common.localconstants import SMTP_SENDER, SMTP_SERVER


# .............................................................................
class EmailNotifier:
    """Class used to connect to an SMTP server and send emails
    """

    # ....................................
    def __init__(self, server=SMTP_SERVER, from_addr=SMTP_SENDER):
        """Constructor

        Args:
            server: (optional) SMTP server to send email from
            from_addr: (optional) The email address to send emails from
        """
        self.from_addr = from_addr
        self.server = smtplib.SMTP(server)

    # ....................................
    def send_message(self, to_addrs, subject, msg):
        """Sends an email to the specified recipients

        Args:
            to_addrs: List of recipients
            subject: The subject of the email
            msg: The content of the email
        """
        if not isinstance(to_addrs, list):
            to_addrs = [to_addrs]

        mail_msg = (
            'From: {}\r\nTo: {}\r\nSubject: {}\r\n\r\n{}'.format(
                self.from_addr, ', '.join(to_addrs), subject, msg))
        try:
            self.server.sendmail(self.from_addr, to_addrs, mail_msg)
        except Exception as err:
            raise LMError('Failed to send email', err)
