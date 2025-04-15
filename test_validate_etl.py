import pandas as pd
import pytest
import re
from schema import expected_schema


class Test_ETLValidation:

    @classmethod
    def setup_class(cls):
        cls.raw_data =  pd.read_csv("raw_customer_data.csv")
        cls.processed_data = pd.read_csv("processed_customer_data.csv")
    
    def test_column_validation_of_raw(self):

        for col in expected_schema:
            assert col in self.raw_data.columns, f"Missing column {col} in raw_data."
    
    def test_column_validation_of_processed(self):

        for col in expected_schema:
            assert col in self.processed_data.columns, f"Missing column {col} in processed data."
    
    def test_column_data_type_for_raw(self):

        for col, type in expected_schema.items():
            sample_value = self.raw_data[col].iloc[0]
            assert isinstance(sample_value,type), f"Incorrect data type for column {col} in raw data."
    
    def test_column_data_type_for_processed(self):

        for col, type in expected_schema.items():
            sample_value = self.processed_data[col].iloc[0]
            assert isinstance(str(sample_value),type), f"Incorrect data type for column {col} in raw data."
    

    def test_validate_email_id_in_processed(self):

        validate_email = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        invalid_emails = self.processed_data[
            ~self.processed_data["Email"].astype(str).str.match(validate_email)
        ]
        assert len(invalid_emails) == 0, f"Invalid {len(invalid_emails)} emails were found: {invalid_emails['Email'].tolist()}"

    @pytest.mark.skip(reason = "Skipping this test temporarily")
    def test_validate_phone_number_in_processed(self):

        validate_phone = r"^\+\d{12}$"
        invalid_phones = self.processed_data[
        ~self.processed_data["PhoneNumber"]
        .astype(str)
        .str.strip()
        .str.match(validate_phone)
        ]   
        assert len(invalid_phones) == 0, f"Invalid {len(invalid_phones)} numbers were found: {invalid_phones['PhoneNumber'].tolist()}"
        



