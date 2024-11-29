from dataclasses import dataclass
from typing import Optional
from pyflink.table import Row
from datetime import datetime
import json

@dataclass
class Transaction:
    trans_date_trans_time: datetime
    cc_num: str
    merchant: str
    category: str
    amt: float
    first: str
    last: str
    gender: str
    street: str
    city: str
    state: str
    zip: int
    lat: float
    long: float
    city_pop: int
    job: str
    dob: datetime
    trans_num: str
    unix_time: int
    merch_lat: float
    merch_long: float
    is_fraud: int

    @staticmethod
    def from_list(data: list) -> "Transaction":
        """
        Creates a Transaction instance from a list of values.
        Assumes the data list is in the same order as the dataclass fields.
        """
        return Transaction(
            trans_date_trans_time=datetime.strptime(data[1], '%Y-%m-%d %H:%M:%S'),
            cc_num=data[2],
            merchant=data[3],
            category=data[4],
            amt=float(data[5]),
            first=data[6],
            last=data[7],
            gender=data[8],
            street=data[9],
            city=data[10],
            state=data[11],
            zip=int(data[12]),
            lat=float(data[13]),
            long=float(data[14]),
            city_pop=int(data[15]),
            job=data[16],
            dob=datetime.strptime(data[17], '%Y-%m-%d'),
            trans_num=data[18],
            unix_time=int(data[19]),
            merch_lat=float(data[20]),
            merch_long=float(data[21]),
            is_fraud=int(data[22])
        )

    def to_row(self) -> Row:
        """
        Converts the Transaction instance to a PyFlink Row.
        """
        return Row(
            trans_date_trans_time=self.trans_date_trans_time,
            cc_num=self.cc_num,
            merchant=self.merchant,
            category=self.category,
            amt=self.amt,
            first=self.first,
            last=self.last,
            gender=self.gender,
            street=self.street,
            city=self.city,
            state=self.state,
            zip=self.zip,
            lat=self.lat,
            long=self.long,
            city_pop=self.city_pop,
            job=self.job,
            dob=self.dob,
            trans_num=self.trans_num,
            unix_time=self.unix_time,
            merch_lat=self.merch_lat,
            merch_long=self.merch_long,
            is_fraud=self.is_fraud
        )

    @staticmethod
    def from_json(json_data: str) -> "Transaction":
        """
        Converts a JSON string to a Transaction instance.
        Sample: {"": "3604", "trans_date_trans_time": "2020-06-22 11:59:51",
        "cc_num": "4874017206859125", "merchant": "fraud_Cassin-Harvey",
        "category": "grocery_net", "amt": "65.97", "first": "Lauren", "last":
        "Williams", "gender": "F", "street": "065 Jones Stravenue", "city":
        "Lake Oswego", "state": "OR", "zip": "97034", "lat": "45.4093", "long":
        "-122.6847", ^Ccategory": "personal_care", "amt": "29.12", "first":
        "Kristina", "last": "Lewis", "gender": "F", "street": "5449 Brandi
        Heights Apt. 111", "city": "Tulsa", "state": "OK", "zip": "74130",
        "lat": "36.2395", "long": "-95.9596", "city_pop": "413574", "job":
        "Bookseller", "dob": "1968-06-18", "trans_num":
        "ab780a837fdce34eea6eecfe85917fc3", "unix_time": "1371903282",
        "merch_lat": "35.288897", "merch_long": "-96.00399300000001",
        "is_fraud": "0"}
        """
        try:
            # Parse the JSON string into a dictionary
            data = json.loads(json_data)

            # Convert the necessary fields
            return Transaction(
                trans_date_trans_time=datetime.strptime(data["trans_date_trans_time"], '%Y-%m-%d %H:%M:%S'),
                cc_num=data["cc_num"],
                merchant=data["merchant"],
                category=data["category"],
                amt=float(data["amt"]),
                first=data["first"],
                last=data["last"],
                gender=data["gender"],
                street=data["street"],
                city=data["city"],
                state=data["state"],
                zip=int(data["zip"]),
                lat=float(data["lat"]),
                long=float(data["long"]),
                city_pop=int(data["city_pop"]),
                job=data["job"],
                dob=datetime.strptime(data["dob"], '%Y-%m-%d'),
                trans_num=data["trans_num"],
                unix_time=int(data["unix_time"]),
                merch_lat=float(data["merch_lat"]),
                merch_long=float(data["merch_long"]),
                is_fraud=int(data["is_fraud"])
            )
        except KeyError as e:
            raise ValueError(f"Missing field in input JSON: {e}")
        except Exception as e:
            raise ValueError(f"Error parsing JSON data: {e}")

    def to_json(self):
        """
        Converts the Transaction instance to a JSON string.
        Returns:
            str: A JSON string representing the Transaction instance.
        """
        return json.dumps({
            "trans_date_trans_time": self.trans_date_trans_time.strftime('%Y-%m-%d %H:%M:%S'),
            "cc_num": self.cc_num,
            "merchant": self.merchant,
            "category": self.category,
            "amt": self.amt,
            "first": self.first,
            "last": self.last,
            "gender": self.gender,
            "street": self.street,
            "city": self.city,
            "state": self.state,
            "zip": self.zip,
            "lat": self.lat,
            "long": self.long,
            "city_pop": self.city_pop,
            "job": self.job,
            "dob": self.dob.strftime('%Y-%m-%d'),
            "trans_num": self.trans_num,
            "unix_time": self.unix_time,
            "merch_lat": self.merch_lat,
            "merch_long": self.merch_long,
            "is_fraud": self.is_fraud
        })

