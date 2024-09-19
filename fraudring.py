import os
import sys
from rule_engine import RuleEngine

from query_handler import SQLDatabaseHandler
from passenger_gang import Passenger_gang
from driver_gang import Driver_gang

from datetime import date, timedelta, datetime
from minio import Minio
import pandas as pd
import logging

class Fraudring:
    def __init__(self,table_names,fraud_table_names,query_thresholds,frequency_fraud_rules,db_name):
        self.db = SQLDatabaseHandler(
            user=os.getenv('DB_USER2'),
            password=os.getenv('DB_PASSWORD2'),
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT')))

        self.fraud_db = SQLDatabaseHandler(
            user=os.getenv('FRAUD2_DB_USER1'),
            password=os.getenv('FRAUD2_DB_PASSWORD1'),
            host=os.getenv('FRAUD2_DB_HOST'),
            port=int(os.getenv('FRAUD2_DB_PORT')),
            database=str(f'{db_name}'))

        self.table_names = table_names
        self.fraud_table_names = fraud_table_names
        self.query_thresholds = query_thresholds
        self.frequency_fraud_rules = frequency_fraud_rules

        self.minioClient = Minio(
            os.getenv('MINIO_DB_HOST'),
            access_key=os.getenv('MINIO_DB_USER2'),
            secret_key=os.getenv('MINIO_DB_PASSWORD2'),
            secure=True)
        self.db_name = db_name

    def get_last_days_passengers(self) -> pd.DataFrame:
        passengers, column_names = self.db.execute_query(f"""
        select p.id, p.comapny_id 
        from {self.table_names['passengers']} p
        where p.created_at BETWEEN DATE(DATE_SUB(NOW(), INTERVAL {self.query_thresholds['passengers_lookback_days']} DAY)) AND DATE(NOW())
        and comapny_id !=0""")
        return pd.DataFrame(passengers, columns=column_names)

    def calculate_drivers_performance_in_db(self):
        self.fraud_db.execute_query(f"""
            INSERT INTO {self.db_name}.{self.fraud_table_names['driver_performance_table']}
                (driver_id,driver_ride_count,percent,created_at,city_id)
                    (select 
                        core.driver_id AS driver_id,
                        count(core.id) driver_ride_count,
                        ((count(distinct(core.passenger_id))) / (count(core.id)) * 100) as percent,
                        Date(SUBDATE(NOW(),1)) as created_at,
                        Max(core.city_id) as city_id
        
                    from 
                        (SELECT r.id, r.driver_id , r.passenger_id,r.city_id
                        FROM  {self.db_name}.{self.fraud_table_names['rides_table']} r 
                            LEFT JOIN {self.db_name}.{self.fraud_table_names['passengers_table']} p 
                                ON p.id = r.passenger_id
                        WHERE  p.comapny_id IS NULL) as core
                    group by core.driver_id);""")

    def insert_filtered_drivers_uniqpass(self):
        self.fraud_db.execute_query(f"""
                                    INSERT INTO {self.db_name}.{self.fraud_table_names['uniqpass_table']} 
                                        (driver_id,driver_ride_count,percent,created_at,city_id)
                                            (select dp.driver_id,dp.driver_ride_count,dp.percent,dp.created_at,dp.city_id
                                            from {self.db_name}.{self.fraud_table_names['driver_performance_table']} as dp
                                            where (dp.percent < {self.query_thresholds['less_uniqpass_rate']} or dp.driver_ride_count > {self.query_thresholds['more_ride_count']})); """)

    def get_passenger_performance(self) -> pd.DataFrame:
        passengers_performance, column_names = self.fraud_db.execute_query(f"""
            select 
                core.passenger_id AS passenger_id,
                count(core.id) as passenger_ride_count,
                ((count(distinct(core.driver_id))) / (count(core.id)) * 100) as percent
            from 
                (SELECT r.id, r.driver_id , r.passenger_id 
                FROM  {self.db_name}.{self.fraud_table_names['rides_table']} r 
                    LEFT JOIN {self.db_name}.{self.fraud_table_names['passengers_table']} p 
                        ON p.id = r.passenger_id 
                WHERE  p.comapny_id IS NULL) as core
            group by core.passenger_id
            having percent < {self.query_thresholds['less_uniq_driver_rate']};""")

        passengers_performance_df = pd.DataFrame(passengers_performance, columns=column_names)
        created_at = str(datetime.now().replace(hour=0, minute=0, second=0) - pd.Timedelta(str(1) + ' days'))[:19]
        passengers_performance_df['created_at'] = created_at
        return passengers_performance_df

    def get_passenger_driver_profile(self) -> pd.DataFrame:
        passenger_driver_profile, column_names = self.fraud_db.execute_query(f"""
            select  r.passenger_id,
                    r.driver_id , 
                    count(*) AS count_freq,
                    vu.percent as percent_uniqdriv,
                    u.percent  as percent_uniqpass,
                    vu.passenger_ride_count ,
                    u.driver_ride_count,
                    date (max(r.created_at)) as created_at 
            from {self.db_name}.{self.fraud_table_names['rides_table']} r
                join {self.db_name}.{self.fraud_table_names['uniqdriv_table']} as vu 
                    ON vu.passenger_id = r.passenger_id
                        left join {self.db_name}.{self.fraud_table_names['uniqpass_table']} u 
                            on u.driver_id = r.driver_id 
             where vu.percent < {self.query_thresholds['less_uniq_driver_rate']} 
            group by r.driver_id , r.passenger_id, vu.percent, 
                u.percent, vu.passenger_ride_count , u.driver_ride_count
            HAVING  count_freq > {self.query_thresholds['more_cout_freq']} and created_at = DATE_SUB(CURDATE(), INTERVAL 1 DAY)""")

        # check threshold edit created_at
        return pd.DataFrame(passenger_driver_profile, columns=column_names)

    def passenger_driver_profile_data_engineering(self, passenger_driver_profile: pd.DataFrame) -> pd.DataFrame:
         
        passenger_expected_unique_driver = passenger_driver_profile['passenger_ride_count'] - passenger_driver_profile[
            'count_freq']+1
        passenger_actual_unique_driver = passenger_driver_profile['passenger_ride_count'] * \
                                                passenger_driver_profile['percent_uniqdriv'] 

        
        passenger_driver_profile[
            'percent_uniqdriv_without_driver'] = passenger_actual_unique_driver / passenger_expected_unique_driver 
        passenger_driver_profile['freq_share'] = (passenger_driver_profile['count_freq'] / passenger_driver_profile[
            'passenger_ride_count']) * 100
    
        return passenger_driver_profile

    def check_profile_fraud_rules(self, passenger_driver_profile: pd.DataFrame):
        rule_engine = RuleEngine(passenger_driver_profile)
        is_fraud = rule_engine.apply(self.frequency_fraud_rules)
        passenger_driver_profile['is_fraud'] = is_fraud
        return passenger_driver_profile

    def get_suspected_rides(self) -> pd.DataFrame:
        suspected_rides, column_names = self.fraud_db.execute_query(f"""
            select r.id,
                r.driver_id,
                r.passenger_id,
                r.created_at
            from {self.db_name}.{self.fraud_table_names['rides_today_table']} as r
                join {self.db_name}.{self.fraud_table_names['uniqpass_table']}  as u
                    on r.driver_id = u.driver_id
                    LEFT JOIN {self.db_name}.{self.fraud_table_names['passengers_table']} as p
                        ON p.id = r.passenger_id 
            WHERE p.comapny_id IS NULL""")
        suspected_rides_df = pd.DataFrame(suspected_rides, columns=column_names)
        suspected_rides_df = suspected_rides_df.drop_duplicates(subset=['id'])
        suspected_rides_df = suspected_rides_df.reset_index(drop=True)
        return suspected_rides_df

    def get_mutual_passengers_today(self) -> pd.DataFrame:
        mutual_passengers_today, column_names = self.fraud_db.execute_query(f"""
            with tb as (select *
                        from {self.db_name}.{self.fraud_table_names['suspected_rides_table']}),
                tb1 as (select * from tb),
                tb2 as (select * from tb)               	
            select  tb1.driver_id as driver_id_1,
                    tb2.driver_id as driver_id_2,
                    tb1.passenger_id as passenger_id,
                    tb1.created_at as created_at 
            from tb1 join tb2
                        on tb1.passenger_id = tb2.passenger_id
            where tb1.id <> tb2.id
                and tb1.driver_id <= tb2.driver_id;""")
        mutual_passengers_today_df = pd.DataFrame(mutual_passengers_today, columns=column_names)
        mutual_passengers_today_df = mutual_passengers_today_df.sort_values('created_at').drop_duplicates(['driver_id_1','driver_id_2', 'passenger_id'],keep='last')
        # why drop(today's data and same_pass is checked in other processes?)?
        mutual_passengers_today_df = mutual_passengers_today_df.reset_index(drop=True)
        return mutual_passengers_today_df

    def get_updated_mutual_passengers(self, mutual_passengers_today_df: pd.DataFrame):
        mutual_passengers, column_names = self.fraud_db.execute_query(
            f"""select * from {self.db_name}.{self.fraud_table_names['mutual_passengers_table']}""")
        mutual_passengers_df = pd.DataFrame(mutual_passengers, columns=column_names)
        mutalpass_intersec = pd.merge(mutual_passengers_df, mutual_passengers_today_df, how='outer',
                                      on=['driver_id_1', 'driver_id_2', 'passenger_id'])

        update = mutalpass_intersec.loc[(mutalpass_intersec['id'].isnull() == False) & (mutalpass_intersec['created_at_y'].isnull() == False)]
        update['updated_at'] = update['created_at_y']
        update = update.drop(['created_at_y'], axis=1)
        update.rename(columns={'created_at_x':'created_at'},inplace=True)
        update = update.sort_values('updated_at').drop_duplicates(['driver_id_1','driver_id_2', 'passenger_id'],keep='last')

        insert = mutalpass_intersec.loc[(mutalpass_intersec['id'].isnull())]
        insert.rename(columns={'created_at_x': 'created_at'}, inplace=True)
        insert['created_at'] = insert['created_at_y']
        insert['updated_at'] = insert['created_at_y']
        insert = insert.drop(['created_at_y'], axis=1)

        notchange = mutalpass_intersec.loc[(mutalpass_intersec['created_at_y'].isnull())]
        notchange = notchange.drop(['created_at_y'], axis=1)
        notchange.rename(columns={'created_at_x': 'created_at'}, inplace=True)
        updated_mutual_passengers = pd.concat([update, insert, notchange], axis=0)
        updated_mutual_passengers = updated_mutual_passengers.sort_values('updated_at').drop_duplicates(['driver_id_1','driver_id_2', 'passenger_id'],keep='last')
        updated_mutual_passengers = updated_mutual_passengers.drop(['id'], axis=1)
        return updated_mutual_passengers

    def get_drivers_intersections_today(self) -> pd.DataFrame:
        drivers_intersections_today, column_names = self.fraud_db.execute_query(f"""
            select  vmp.driver_id_1,
                    vmp.driver_id_2, 
                    count(vmp.passenger_id) as count_samepass,
                    vmp.created_at as created_at
            FROM {self.db_name}.{self.fraud_table_names['mutual_passengers_table']} vmp
            where vmp.updated_at BETWEEN DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)) AND DATE(NOW())
            group by vmp.driver_id_1,
                     vmp.driver_id_2,
                     vmp.created_at ;""")
        drivers_intersections_today_df = pd.DataFrame(drivers_intersections_today, columns=column_names)
        return drivers_intersections_today_df

    def get_updated_drivers_intersections(self, drivers_intersections_today_df: pd.DataFrame,updated_mutual_passengers:pd.DataFrame):
        drivers_intersections, column_names = self.fraud_db.execute_query(
            f"""select * from {self.db_name}.{self.fraud_table_names['intersections_table']}""")
        drivers_intersections_df = pd.DataFrame(drivers_intersections, columns=column_names)
        drivers_intersections_df = drivers_intersections_df.drop_duplicates(
            subset=['driver_id_1', 'driver_id_2', 'count_samepass'])
        drivers_intersections_df = drivers_intersections_df.reset_index(drop=True)

        drivers_intersections_today_df.rename(
            columns={'count_samepass': 'count_samepass_today', 'created_at': 'created_at_today'}, inplace=True)
        drivers_intersections_df = pd.merge(drivers_intersections_df, drivers_intersections_today_df, how='outer',
                                            on=['driver_id_1', 'driver_id_2'])

        update = drivers_intersections_df.loc[(drivers_intersections_df['id'].isnull() == False) & (drivers_intersections_df['count_samepass_today'].isnull() == False)]

        update['samepass'] = 0
        update['updated_at'] = update['created_at_today']
        update = update[[ 'driver_id_1', 'driver_id_2', 'samepass', 'created_at', 'updated_at']]
        update.rename(columns={'samepass':'count_samepass'},inplace=True)
        update_history = updated_mutual_passengers.merge(update,how='right',on=['driver_id_1', 'driver_id_2'],suffixes=['_mutual','_update'])
        update_history = update_history.sort_values('updated_at_update').drop_duplicates(['driver_id_1','driver_id_2', 'passenger_id'],keep='last')
        

        update = update_history.groupby(['driver_id_1','driver_id_2']).size().rename('count_samepass')
        update_at = update_history.sort_values('updated_at_update').drop_duplicates(['driver_id_1','driver_id_2'],keep='last').set_index(['driver_id_1','driver_id_2'])['updated_at_update'].rename('updated_at')
        created_at = update_history.sort_values('created_at_update').drop_duplicates(['driver_id_1','driver_id_2'],keep='first').set_index(['driver_id_1','driver_id_2'])['created_at_update'].rename('created_at')
        update = pd.concat([update,created_at,update_at],axis=1).reset_index()

        insert = drivers_intersections_df.loc[(drivers_intersections_df['id'].isnull())]
        insert = insert[['driver_id_1', 'driver_id_2', 'count_samepass_today', 'created_at_today']]
        insert.rename(columns={'created_at_today': 'created_at', 'count_samepass_today': 'count_samepass'},
                      inplace=True)
        insert['updated_at'] = insert['created_at']

        notchange = drivers_intersections_df.loc[(drivers_intersections_df['count_samepass_today'].isnull())]
        notchange = notchange[['driver_id_1', 'driver_id_2', 'count_samepass', 'created_at', 'updated_at']]

        drivers_intersections_df = pd.concat([update, insert, notchange], axis=0)
        drivers_intersections_df = drivers_intersections_df.drop_duplicates(
            subset=['driver_id_1', 'driver_id_2', 'count_samepass'])
        drivers_intersections_df = drivers_intersections_df.reset_index(drop=True)

        return drivers_intersections_df

    def get_fraud_history_rides(self) -> pd.DataFrame:
        fraud_rides, column_names = self.fraud_db.execute_query(f"""
            select r.id as ride_id,
                r.driver_id,
                r.passenger_id,
                r.created_at,
                fh.created_at as fraud_date
            from {self.db_name}.{self.fraud_table_names['fraud_history_table']} fh 
                left join {self.db_name}.{self.fraud_table_names['rides_table']} r 
                    on r.driver_id = fh.driver_id and r.passenger_id = fh.passenger_id 
            where fh.is_fraud = 1 
                and fh.created_at BETWEEN DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)) AND DATE(NOW())
                and r.created_at BETWEEN DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)) AND DATE(NOW());
                """)
        return pd.DataFrame(fraud_rides, columns=column_names)

    def get_gang_rides(self, unique_drivers: tuple):
        # why not use rides_today?
        fraud_rides, column_names = self.fraud_db.execute_query(f""" 
            select 
                id as ride_id,
                driver_id,
                passenger_id,
                created_at
            from {self.db_name}.{self.fraud_table_names['rides_table']} r 
            where driver_id in {unique_drivers}
                 and created_at BETWEEN DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)) AND DATE(NOW());""")
        fraud_rides_df = pd.DataFrame(fraud_rides, columns=column_names)
        fraud_rides_df['fraud_date'] = date.today() - timedelta(days=1)
        return fraud_rides_df

    def get_analized_drivers(self, unique_drivers: tuple) -> tuple:
        driver_ids_1_2, column_names = self.fraud_db.execute_query(f""" 
            select 
                i.driver_id_1,
                i.driver_id_2
            from {self.db_name}.{self.fraud_table_names['intersections_table']} as i
            where (i.driver_id_1 in {unique_drivers} or i.driver_id_2 in {unique_drivers})
                and i.count_samepass > {self.query_thresholds['more_count_samepass_edge']}""")

        # is this threshold set correctly
        driver_ids_1_2_df = pd.DataFrame(driver_ids_1_2, columns=column_names)
        driver_id1 = driver_ids_1_2_df['driver_id_1'].to_list()
        driver_id2 = driver_ids_1_2_df['driver_id_2'].to_list()
        return tuple(set(driver_id1 + driver_id2))

    def get_analized_passengers(self, unique_drivers: tuple) -> tuple:
        passenger_ids, column_names = self.fraud_db.execute_query(f""" 
            select passenger_id,
                created_at
            from {self.db_name}.{self.fraud_table_names['mutual_passengers_table']} mp 
            where   (mp.driver_id_1 in {unique_drivers} or mp.driver_id_2 in {unique_drivers}) and
                        mp.created_at BETWEEN DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)) AND DATE(NOW());""")
        # add threshold
        passenger_ids_df = pd.DataFrame(passenger_ids, columns=column_names)
        passenger_ids = passenger_ids_df['passenger_id'].to_list()
        return tuple(set(passenger_ids))

    def get_suspected_passengers_rides(self, unique_passengers: tuple) -> pd.DataFrame:
        suspect_rides, column_names = self.fraud_db.execute_query(f""" 
            select 
                r.id,
                r.driver_id,
                r.passenger_id,
                r.service_type,
                r.city_id,
                r.created_at
            from {self.db_name}.{self.fraud_table_names['rides_table']} as r
            where r.passenger_id in {unique_passengers};""")
        return pd.DataFrame(suspect_rides, columns=column_names)

    def filter_suspected_rides(self, suspect_rides: pd.DataFrame) -> pd.DataFrame:
        passenger_rides = suspect_rides.groupby('passenger_id').size()
        passenger_rides = passenger_rides[passenger_rides > self.query_thresholds['more_passenger_ride_count']]
        passengers = tuple(set(list(passenger_rides.index)))
        return suspect_rides[suspect_rides['passenger_id'].isin(passengers)]
        # add threshold

    def get_mutual_drivers(self) -> pd.DataFrame:
        mutual_drivers, column_names = self.fraud_db.execute_query(f"""
            with tb as (select *
                        from {self.db_name}.{self.fraud_table_names['filtered_suspassdriv_rides_table']}),
                tb1 as (select *
                        from tb
                        ),
                tb2 as (select *
                        from tb)               	
            select tb1.passenger_id as passenger_id_1,
                    tb2.passenger_id as passenger_id_2,
                    tb1.driver_id as driver_id
            from tb1 join tb2
                        on tb1.driver_id = tb2.driver_id
            where tb1.id <> tb2.id
                    and tb1.passenger_id <= tb2.passenger_id;""")
        mutual_drivers_df = pd.DataFrame(mutual_drivers, columns=column_names)
        mutual_drivers_df = mutual_drivers_df.drop_duplicates(subset=['passenger_id_1', 'passenger_id_2', 'driver_id'])
        return mutual_drivers_df

    def get_intersections_passengers(self) -> pd.DataFrame:
        intersections_passengers, column_names = self.fraud_db.execute_query(f"""
                select vmp.passenger_id_1 , vmp.passenger_id_2 , count(vmp.driver_id) as count_samedriv
                FROM {self.db_name}.{self.fraud_table_names['mutual_drivers_table']} vmp
                group by vmp.passenger_id_1 , vmp.passenger_id_2
                ;""")
        intersections_passengers_df = pd.DataFrame(intersections_passengers, columns=column_names)
        return intersections_passengers_df

    def get_filtered_gang_rides(self, unique_gang_drivers, unique_gang_passengers) -> pd.DataFrame:
        filtered_gang_rides, column_names = self.fraud_db.execute_query(f"""
            select r.id,r.driver_id,r.passenger_id,r.city_id,r.created_at 
            from {self.db_name}.rides as r
            where r.created_at BETWEEN DATE(DATE_SUB(NOW(), INTERVAL 1 DAY)) AND DATE(NOW())
            and r.driver_id in {unique_gang_drivers}
            and r.passenger_id in {unique_gang_passengers}
            ;""")
        filtered_gang_rides_df = pd.DataFrame(filtered_gang_rides, columns=column_names)
        filtered_gang_rides_df = filtered_gang_rides_df.drop_duplicates(subset='id')
        return filtered_gang_rides_df

    def run(self):
        try:
            
            logging.warning('%s' %(str(datetime.now())+': Corporate customers has started'))
            passengers_df = self.get_last_days_passengers()
            self.fraud_db.export_df(passengers_df, self.fraud_table_names['passengers_table'])
            del passengers_df
            logging.warning('%s' %(str(datetime.now())+': Corporate customers has finished'))

            logging.warning('%s' %(str(datetime.now())+': Calculating drivers_performances has started'))
            self.fraud_db.execute_query(
                f"""truncate table {self.db_name}.{self.fraud_table_names['driver_performance_table']};""")
            self.calculate_drivers_performance_in_db()
            logging.warning('%s' %(str(datetime.now())+': Calculating drivers_performances has finished'))

            logging.warning('%s' %(str(datetime.now())+': filtring drivers has started'))
            self.fraud_db.execute_query(f"""truncate table {self.db_name}.{self.fraud_table_names['uniqpass_table']};""")
            self.insert_filtered_drivers_uniqpass()
            logging.warning('%s' %(str(datetime.now())+': filtring drivers has finished'))
            

            ## changed order
            logging.warning('%s' %(str(datetime.now())+': Calculating passengers_performances has started'))
            self.fraud_db.execute_query(f"""truncate table {self.db_name}.{self.fraud_table_names['uniqdriv_table']}""")
            passengers_performance_df = self.get_passenger_performance()
            self.fraud_db.export_df(passengers_performance_df, self.fraud_table_names['uniqdriv_table'])
            logging.warning('%s' %(str(datetime.now())+': Calculating passengers_performances has finished'))

            logging.warning('%s' %(str(datetime.now())+': importing passenger_driver profiles has started'))
            passenger_driver_profile_df = self.get_passenger_driver_profile()
            logging.warning('%s' %(str(datetime.now())+': importing passenger_driver profiles has finished'))

            logging.warning('%s' %(str(datetime.now())+': data enginnering on passenger_driver profiles has started'))
            passenger_driver_profile_df = self.passenger_driver_profile_data_engineering(passenger_driver_profile_df)
            logging.warning('%s' %(str(datetime.now())+': data enginnering on passenger_driver profiles has finished'))

            logging.warning('%s' %(str(datetime.now())+': checking fraud rules on passenger_driver profiles has started'))
            passenger_driver_profile_df = self.check_profile_fraud_rules(passenger_driver_profile_df)
            logging.warning('%s' %(str(datetime.now())+': checking fraud rules on passenger_driver profiles has finished'))


            passenger_driver_profile_df = passenger_driver_profile_df[['passenger_id',
                                                                    'driver_id',
                                                                    'count_freq',
                                                                    'percent_uniqdriv',
                                                                    'percent_uniqpass',
                                                                    'passenger_ride_count',
                                                                    'driver_ride_count',
                                                                    'freq_share',
                                                                    'is_fraud',
                                                                    'created_at']]
            passenger_driver_profile_df.drop_duplicates(subset = ['driver_id'],inplace = True)
            
            logging.warning('%s' %(str(datetime.now())+': exporting fraud_history has started'))
            self.fraud_db.export_df(passenger_driver_profile_df, self.fraud_table_names['fraud_history_table'])
            del passenger_driver_profile_df
            logging.warning('%s' %(str(datetime.now())+': exporting fraud_history has finished'))

            logging.warning('%s' %(str(datetime.now())+': exporting fraud_rides has started'))
            fraud_rides_df = self.get_fraud_history_rides()
            self.fraud_db.export_df(fraud_rides_df, self.fraud_table_names['fraud_rides_table'])
            del fraud_rides_df
            logging.warning('%s' %(str(datetime.now())+': exporting fraud_rides has finished'))

            ## changed_order

            logging.warning('%s' %(str(datetime.now())+': calculating suspected_rides has started'))
            suspected_rides_df = self.get_suspected_rides()
            self.fraud_db.execute_query(
                f'''truncate table {self.db_name}.{self.fraud_table_names['suspected_rides_table']};''')
            self.fraud_db.export_df(suspected_rides_df, self.fraud_table_names['suspected_rides_table'])
            del suspected_rides_df
            logging.warning('%s' %(str(datetime.now())+': calculating suspected_rides has finished'))
            
            logging.warning('%s' %(str(datetime.now())+': mutual_passengers_today has started'))
            mutual_passengers_today_df = self.get_mutual_passengers_today()
            self.fraud_db.execute_query(
                f'''truncate table {self.db_name}.{self.fraud_table_names['mutual_passengers_today_table']};''')
            self.fraud_db.export_df(mutual_passengers_today_df, self.fraud_table_names['mutual_passengers_today_table'])
            logging.warning('%s' %(str(datetime.now())+': mutual_passengers_today has finished'))

            logging.warning('%s' %(str(datetime.now())+': mutual_passengers has started'))
            updated_mutual_passengers_df = self.get_updated_mutual_passengers(mutual_passengers_today_df)
            del mutual_passengers_today_df
            self.fraud_db.execute_query(
                f"""truncate table {self.db_name}.{self.fraud_table_names['mutual_passengers_table']};""")
            self.fraud_db.export_df(updated_mutual_passengers_df, self.fraud_table_names['mutual_passengers_table'])
            
            logging.warning('%s' %(str(datetime.now())+': mutual_passengers has finished'))

            logging.warning('%s' %(str(datetime.now())+': intersections_today has started'))
            drivers_intersections_today_df = self.get_drivers_intersections_today()
            self.fraud_db.execute_query(
                f"""truncate table {self.db_name}.{self.fraud_table_names['todayintersections_table']};""")
            self.fraud_db.export_df(drivers_intersections_today_df, self.fraud_table_names['todayintersections_table'])
            logging.warning('%s' %(str(datetime.now())+': intersections_today has finished'))

            logging.warning('%s' %(str(datetime.now())+': intersections has started'))
            updated_drivers_intersections_df = self.get_updated_drivers_intersections(drivers_intersections_today_df,updated_mutual_passengers_df)
            del updated_mutual_passengers_df
            del drivers_intersections_today_df
            self.fraud_db.execute_query(f"""truncate table {self.db_name}.{self.fraud_table_names['intersections_table']};""")
            self.fraud_db.export_df(updated_drivers_intersections_df, self.fraud_table_names['intersections_table'])
            del updated_drivers_intersections_df
            logging.warning('%s' %(str(datetime.now())+': intersections has finished'))

            logging.warning('%s' %(str(datetime.now())+':  gang driver started'))
            driver_gang = Driver_gang(self.fraud_db,
                                    fraud_table_names={key: value for key, value in self.fraud_table_names.items()
                                                        if key in ('intersections_table',
                                                                    'uniqpass_table',
                                                                    'cities_table')},
                                    thresholds={key: value for key, value in self.query_thresholds.items()
                                                if key in ('more_count_samepass_edge',
                                                            'more_driver_gang_size',
                                                            'more_unique_driv_per_city')})

            driver_gangs_df = driver_gang.find_gangs()
            self.fraud_db.export_df(driver_gangs_df, self.fraud_table_names['gang_table'])
            logging.warning('%s' %(str(datetime.now())+':  gang driver finished'))

            logging.warning('%s' %(str(datetime.now())+':  gang ride driver started'))
            unique_gang_drivers = tuple(set(driver_gangs_df['driver_id'].tolist()))
            gang_rides_df = self.get_gang_rides(unique_gang_drivers)
            print(1)
            unique_gang_drivers_today = tuple(set(gang_rides_df['driver_id'].tolist()))
            gang_daily_df = driver_gangs_df[driver_gangs_df['driver_id'].isin(unique_gang_drivers_today)]
            print(2)
            self.fraud_db.export_df(gang_daily_df, self.fraud_table_names['gang_daily_table'])
            del driver_gangs_df
            del gang_daily_df
            self.fraud_db.export_df(gang_rides_df, self.fraud_table_names['gang_rides_table'])
            del gang_rides_df
            logging.warning('%s' %(str(datetime.now())+':  gang rides driver finished'))
            self.fraud_db.execute_query(f"""truncate table {self.db_name}.{self.fraud_table_names['rides_today_table']}""")
            
            logging.warning('%s' %(str(datetime.now())+':  importing analyzed drivers started'))
            analyzed_drivers = self.get_analized_drivers(unique_gang_drivers_today)
            logging.warning('%s' %(str(datetime.now())+':  importing analyzed drivers finished'))
            
            logging.warning('%s' %(str(datetime.now())+':  importing analyzed passengers started'))
            unique_passengers = self.get_analized_passengers(analyzed_drivers)
            logging.warning('%s' %(str(datetime.now())+':  importing analyzed passengers started'))
            
            logging.warning('%s' %(str(datetime.now())+':  calculating suspected rides started'))
            suspected_rides_df = self.get_suspected_passengers_rides(unique_passengers)
            

            suspected_rides_df = self.filter_suspected_rides(suspected_rides_df)
            
            suspected_passengers_df = suspected_rides_df[['passenger_id', 'city_id']]
            suspected_passengers_df = suspected_passengers_df.drop_duplicates(subset=['passenger_id', 'city_id'])
            suspected_passengers_df['created_at'] = datetime.today().strftime('%Y-%m-%d')
           
            suspected_rides_df = suspected_rides_df[suspected_rides_df['driver_id'].isin(analyzed_drivers)]
            suspected_rides_df = suspected_rides_df.drop_duplicates(subset=['driver_id','passenger_id'])
            

            self.fraud_db.execute_query(
                f"""truncate table {self.db_name}.{self.fraud_table_names['suspect_passengers_table']}""")
            self.fraud_db.export_df(suspected_passengers_df, self.fraud_table_names['suspect_passengers_table'])
            self.fraud_db.execute_query(
                f"""truncate table {self.db_name}.{self.fraud_table_names['filtered_suspassdriv_rides_table']}""")
            self.fraud_db.export_df(suspected_rides_df, self.fraud_table_names['filtered_suspassdriv_rides_table'])
            logging.warning('%s' %(str(datetime.now())+':  calculating suspected rides finished'))

            logging.warning('%s' %(str(datetime.now())+':  calculating mutual drivers started'))
            mutual_drivers_df = self.get_mutual_drivers()
            self.fraud_db.execute_query(
                f"""truncate table {self.db_name}.{self.fraud_table_names['mutual_drivers_table']}""")
            self.fraud_db.export_df(mutual_drivers_df, self.fraud_table_names['mutual_drivers_table'])            
            mutual_drivers_df['created_at'] = str(datetime.now().replace(hour=0, minute=0, second=0))[:19]
            mutual_drivers_df.reset_index(inplace=True, drop=True)
            self.fraud_db.export_df(mutual_drivers_df, self.fraud_table_names['mutual_drivers_history_table'])
            logging.warning('%s' %(str(datetime.now())+':  calculating mutual drivers finished'))

            logging.warning('%s' %(str(datetime.now())+':  calculating intersections_passengers started'))
            intersections_passengers_df = self.get_intersections_passengers()
            self.fraud_db.execute_query(
                f"""truncate table {self.db_name}.{self.fraud_table_names['intersections_passengers_table']}""")
            self.fraud_db.export_df(intersections_passengers_df, self.fraud_table_names['intersections_passengers_table'])

            intersections_passengers_df['created_at'] = str(datetime.now().replace(hour=0, minute=0, second=0))[:19]
            self.fraud_db.export_df(intersections_passengers_df,
                                    self.fraud_table_names['intersections_passengers_history_table'])
            logging.warning('%s' %(str(datetime.now())+':  calculating intersections_passengers finished'))

            logging.warning('%s' %(str(datetime.now())+':  calculating passengers gangs started'))
            passenger_gang = Passenger_gang(self.fraud_db,
                                            fraud_table_names={key: value for key, value in
                                                            self.fraud_table_names.items()
                                                            if
                                                            key in ('intersections_passengers_table',
                                                                    'suspect_passengers_table',
                                                                    'cities_table')},
                                            thresholds={key: value for key, value in self.query_thresholds.items()
                                                        if key in ('more_count_samedriv_edge',
                                                                'more_passenger_gang_size',
                                                                'more_unique_pass_per_city')})

            passenger_gangs_df = passenger_gang.find_gangs()
            self.fraud_db.export_df(passenger_gangs_df, self.fraud_table_names['gang_passengers_table'])
            logging.warning('%s' %(str(datetime.now())+':  calculating passengers gangs finished'))

            logging.warning('%s' %(str(datetime.now())+':  filtering gang rides started'))
            unique_gang_passengers = tuple(set(passenger_gangs_df['passenger_id'].to_list()))
            filtered_gang_rides_df = self.get_filtered_gang_rides(unique_gang_drivers_today, unique_gang_passengers)
            filtered_gang_rides_df['fraud_date'] = date.today() - timedelta(days=1)
            self.fraud_db.export_df(filtered_gang_rides_df, self.fraud_table_names['filtered_gang_rides_table'])
            logging.warning('%s' %(str(datetime.now())+':  filtering gang rides finished'))
            logging.warning('*******************************************')
        except Exception as CodeError:
            import requests
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

            print(exc_type, fname, exc_tb.tb_lineno)
            logging.warning('%s' %(str(datetime.now())+': \n'+str(CodeError)))
            logging.warning('%s' %(str(exc_type)))
            logging.warning('%s' %(str(fname)))
            logging.warning('%s' %(str(exc_tb.tb_lineno)))
                
                
            logging.warning('*******************************************')