from mysql.connector import connect, Error
import logging


class DataAccess:

    def __init__(self, host, port, user, password, database):
        try:
            self.connection = connect(host=host, user=user, password=password, port=port, database=database)
            logging.debug("connected")
        except Error as e:
            logging.exception(f"Failed to connect to {host}", e)
            raise

    def shutdown(self):
        if self.connection:
            logging.debug("closing")
            self.connection.close()

    def get_users_by_lastname_range(self, start, end):
        """query the dox users by range of last names"""
        users = []
        sql = """
        select user.id, user.practice_id, user.firstname, user.lastname, user.specialty, user_practice.location
        from user, user_practice where
        user_practice.id = user.practice_id
        and user.lastname >='%s' and user.lastname <= '%s'
        order by user.lastname, user.firstname
        """ % (
            start,
            end,
        )
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            for user in cursor.fetchall():
                users.append({
                    'id': user[0],
                    'practice_id': user[1],
                    'firstname': user[2],
                    'lastname': user[3],
                    'specialty': user[4],
                    'location': user[5],
                })
        return users

    def save_vendor_users_last_active_date(self, users):
        """upsert the last active date of users at vendor site"""
        logging.debug("saving %s" % len(users))
        sql = """
            insert into user_vendor(user_id, last_active_date_vendor) values(%(user_id)s, %(last_active_date_vendor)s)
             ON DUPLICATE KEY UPDATE last_active_date_vendor=VALUES(last_active_date_vendor);
            
        """
        with self.connection.cursor() as cursor:
            cursor.executemany(sql, users)
            self.connection.commit()
            cursor.close()
