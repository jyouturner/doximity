from dotenv import load_dotenv
import logging
import os
from getpass import getpass
from dao import DataAccess
from vendor import FriendlyVendor
import asyncio
import time

"""
    this script will download the user data from friendly vendor, match to Doximity user, and save the user's last
    active date (at vendors) to our own table.
    
    It will write the loading results to a local file outputs.txt

"""
start_time = time.time()
load_dotenv()

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=LOG_LEVEL)

class VendorUserData:

    def __init__(self):
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        db_user = os.getenv("DB_USER")
        database = os.environ.get('DB_DATABASE')
        password = os.getenv("DB_PASSWORD")
        if not password:
            password = getpass(f"Enter mysql password for user {db_user}")
        self.dao = DataAccess(host, port, db_user, password, database)
        self.load = os.environ.get('LOAD', False)

    def shutdown(self):
        if self.dao:
            self.dao.shutdown()

    async def ingest(self):
        """inject the friendly vendor user data, match to doximity user and store the last active date of the users"""
        vendor = FriendlyVendor()
        # download the first page
        page = await vendor.download_pages(1, 2)
        # ingest by batches
        batch_size = int(os.environ.get('VENDOR_API_PAGES_BATCH_SIZE'))
        total_pages = page[0]['total_pages']
        logging.info(f"totally {total_pages}")
        total_match_count = 0
        try:
            for batch in range(0, 1 + int(total_pages / batch_size)):
                from_page = 1 + batch * batch_size
                end_page = from_page + batch_size
                if end_page >= total_pages:
                    end_page = total_pages + 1
                logging.info(f"batch {batch} from {from_page} to {end_page}")
                pages = await vendor.download_pages(from_page, end_page)
                for page in pages:
                    logging.debug("downloaded data page %s", page['current_page'])
                    match_result = self.match_vendor_users(page['users'])
                    total_match_count += match_result['found']
                    if self.load:
                        # persist the dox user's last active date at vendor site
                        self.dao.save_vendor_users_last_active_date(match_result['dox_users'])
        finally:
            self.shutdown()
            self.outputs("output.txt", total_match_count, match_result)

    def outputs(self, output_file, total_match, match_result):
        """output the required results of the project"""
        with open(output_file, "w") as f:
            f = open(output_file, 'w')
            f.write("Elapsed Time: %s minutes %s seconds\n" % (int((time.time() - start_time)/60), int((time.time() - start_time)%60)))
            f.write("Total Matches: %s\n" % total_match)
            f.write("Sample Output:\n")
            if match_result:
                for index in range(0, 10):
                    f.write("%s\t\n" % match_result['dox_users'][index])
            ddl_sql = """
                create table user_vendor(user_id INT NOT NULL,last_active_date_vendor Date DEFAULT NULL,
                    PRIMARY KEY (user_id),
                    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES user(id)
                );
            """

            f.write("SQL DDL: %s" % ddl_sql)
            f.close()

    def match_vendor_users(self, vendor_users):
        """
        given the list of users from vendor, ordered by lastname, find the matching dox user. We use the vendor user's
        last name range to reduce the query results size.
        :param vendor_users: list of vendor users, ordered by last name
        :return: matching results
        """
        lastname_start = vendor_users[0]['lastname']
        lastname_end = vendor_users[-1]['lastname']
        dox_users = self.dao.get_users_by_lastname_range(lastname_start, lastname_end)
        try:
            return self.match_user_data(dox_users, vendor_users)
        finally:
            pass

    def match_user_data(self, dox_users, vendor_users):
        """
        match the vendor user data to doximity user data, and return the matching results.
        :param dox_users: list of doximity users ordered by last name
        :param vendor_users: list of vendor users ordered by last name
        :return: matching results including statistics and matching dox user ids to persist
        """
        match_result = {
            'total users': len(vendor_users),
            'found': 0,
            'not found': 0,
            'warning': 0,
            'dox_users': []
        }
        index = 0
        for vendor_user in vendor_users:
            lastname = vendor_user['lastname']
            while index < len(dox_users):
                dox_lastname = dox_users[index].get('lastname')
                if dox_lastname == lastname:
                    break
                index += 1
            if index == len(dox_users):
                match_result['not found'] += 1
                continue
            elif lastname == dox_users[index].get('lastname'):
                match_index = index
                while match_index < len(dox_users) and dox_users[match_index].get('lastname') == lastname:
                    dox_user = dox_users[match_index]
                    if self.does_match_vendor_to_dox_user(vendor_user, dox_user):
                        # bingo
                        if 'dox_user_id' in vendor_user:
                            logging.warning("more than one records found matching %s", vendor_user)
                            match_result['warning'] += 1
                        match_result['dox_users'].append({
                            'user_id': dox_user.get('id'),
                            'last_active_date_vendor': vendor_user['last_active_date']
                        })
                        match_result['found'] += 1
                        break
                    match_index += 1
            else:
                match_result['not found'] += 1
                continue
        return match_result

    def does_match_vendor_to_dox_user(self, vendor_user, dox_user):
        """check whether the user data from vendor match the doxitmity user.
        We will compare last name, first name, speciality and location. Note, if dox user location data has space, we
        will replace with underscore.
        :param vendor_user:
        :param dox_user:
        :return: True if two users match
        """
        lastname = vendor_user['lastname']
        firstname = vendor_user['firstname']
        location = vendor_user['practice_location']
        specialty = vendor_user['specialty']
        if dox_user.get('lastname') == lastname and dox_user.get('firstname') == firstname and dox_user.get(
                'specialty') == specialty and dox_user.get('location').replace(' ', '_') == location:
            return True
        else:
            return False


if __name__ == "__main__":
    asyncio.run(VendorUserData().ingest())

