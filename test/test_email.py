from mock import patch
import unittest
import smtplib
import functions as fn
import update_database as ud


class SendEmailTests(unittest.TestCase):

    @patch("smtplib.SMTP")
    def test_send_email_with_decorator(self, mock_smtp):
        # Build test message
        from_address = "from@domain.com"
        to_address = "to@domain.com"

        msg = fn.build_message(
            from_address, [to_address], "subject", "message")

        # Send message
        ud.send_message(msg)

        # Check
        mock_smtp.return_value.sendmail.assert_called_once_with(
            from_address, [to_address], msg.as_string())

    @patch("smtplib.SMTP")
    def test_one_recipient_refused(self, mock_smtp):
        # Build test message
        from_address = "from@domain.com"
        to_addresses = ["first@domain.com", "second@domain.com"]

        msg = fn.build_message(
            from_address, to_addresses, "subject", "message")
        error = {
            to_addresses[0]:
                (450, "Requested mail action not taken: mailbox unavailable")
        }

        # Returns a send failure for the first recipient
        instance = mock_smtp.return_value
        instance.sendmail.return_value = error

        # Call 'send_message' function
        result = ud.send_message(msg)

        # Check returned value
        self.assertIsInstance(result, dict)
        self.assertEqual(result, error)


if __name__ == '__main__':
    unittest.main()
