import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import json

# Import functions from your module
# If your script is named "app.py", then:
# from app import load_data, build_message, send_to_sqs

# For illustration assume functions are defined here:
from main import build_message, send_to_sqs


class TestSchoolMerge(unittest.TestCase):

    def setUp(self):
        # Sample person data simulating person.csv
        self.person_df = pd.DataFrame([
            {
                "name": "Sarah Hughes",
                "weight": 81.9,
                "height": 173.8,
                "school": "Buck, Salazar and Kim"
            }
        ])

        # Sample school data simulating school.csv
        self.school_df = pd.DataFrame([
            {
                "name": "Buck, Salazar and Kim",
                "address": "7416 Turner Heights",
                "city": "South Susan"
            }
        ])

    def test_merge_logic(self):
        # Perform the merge manually the same way the app does
        merged = self.person_df.merge(
            self.school_df,
            left_on="school",
            right_on="name",
            how="left",
            suffixes=("", "_school")
        )

        self.assertEqual(len(merged), 1)
        row = merged.iloc[0]

        # Validate merge fields
        self.assertEqual(row["name"], "Sarah Hughes")
        self.assertEqual(row["school"], "Buck, Salazar and Kim")
        self.assertEqual(row["name_school"], "Buck, Salazar and Kim")
        self.assertEqual(row["address"], "7416 Turner Heights")
        self.assertEqual(row["city"], "South Susan")


class TestMessageBuilder(unittest.TestCase):

    def test_message_building(self):
        row = {
            "name": "Sarah Hughes",
            "weight": 81.9,
            "height": 173.8,
            "name_school": "Buck, Salazar and Kim",
            "address": "7416 Turner Heights",
            "city": "South Susan"
        }

        msg = build_message(row)

        expected = {
            "name": "Sarah Hughes",
            "weight": "81.9",
            "height": "173.8",
            "school_name": "Buck, Salazar and Kim",
            "school_address": "7416 Turner Heights",
            "school_city": "South Susan"
        }

        self.assertEqual(msg, expected)


class TestSQSSend(unittest.TestCase):

    @patch("boto3.client")
    def test_sqs_send(self, mock_boto):

        mock_sqs = MagicMock()
        mock_boto.return_value = mock_sqs

        messages = [
            {
                "name": "Test User",
                "weight": "90.1",
                "height": "180.5",
                "school_name": "Example School",
                "school_address": "123 Road",
                "school_city": "Townsville"
            }
        ]

        send_to_sqs(messages)

        # Validate SQS send was called
        mock_sqs.send_message.assert_called_once()

        call_args = mock_sqs.send_message.call_args[1]

        # MessageBody should be valid JSON
        json.loads(call_args["MessageBody"])


class TestFullFlow(unittest.TestCase):
    """
    Tests main() end-to-end using mocks for:
    - CSV loading
    - SQS sending
    """

    @patch("main.send_to_sqs")
    @patch("main.load_data")
    def test_full_flow(self, mock_load, mock_send):

        # Mock merged DataFrame that main() expects
        mock_merged_df = pd.DataFrame([
            {
                "name": "Sarah Hughes",
                "weight": 81.9,
                "height": 173.8,
                "name_school": "Buck, Salazar and Kim",
                "address": "7416 Turner Heights",
                "city": "South Susan"
            }
        ])

        mock_load.return_value = mock_merged_df

        # Import inside test to prevent premature execution
        import main
        main.main()

        # Ensure load_data was called
        mock_load.assert_called_once()

        # Ensure messages were passed to send_to_sqs
        self.assertTrue(mock_send.called)
        msgs = mock_send.call_args[0][0]
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0]["name"], "Sarah Hughes")


if __name__ == '__main__':
    unittest.main()
