import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components

from datetime import date, timedelta
from query_handler import SQLDatabaseHandler


class Driver_gang():

    def __init__(self, fraud_db: SQLDatabaseHandler, fraud_table_names: dict, thresholds: dict):
        self.fraud_table_names = fraud_table_names
        self.fraud_db = fraud_db
        self.thresholds = thresholds

    def __build_graph(self, city_drivers_intersection: pd.DataFrame) -> "tuple[tuple, csr_matrix]":
        city_drivers_intersection = city_drivers_intersection.sort_values(by=["driver_id_1", "driver_id_2"])
        city_drivers_intersection.reset_index(inplace=True, drop=True)

        drivers_1 = city_drivers_intersection['driver_id_1'].tolist()
        drivers_2 = city_drivers_intersection['driver_id_2'].tolist()
        unique_drivers = tuple(set(drivers_1 + drivers_2))

        city_drivers_intersection['index_1'] = city_drivers_intersection['driver_id_1'].apply(
            lambda x: unique_drivers.index(x))
        city_drivers_intersection['index_2'] = city_drivers_intersection['driver_id_2'].apply(
            lambda x: unique_drivers.index(x))

        city_drivers_intersection = city_drivers_intersection[
            ['driver_id_1', 'driver_id_2', 'count_samepass', 'index_1', 'index_2']]

        graph = csr_matrix((np.ones((len(city_drivers_intersection))),
                            (city_drivers_intersection['index_1'], city_drivers_intersection['index_2'])),
                           shape=(len(unique_drivers), len(unique_drivers)))
        return unique_drivers, graph

    def __find_gang(self, city_drivers: tuple, city_graph: csr_matrix) -> pd.DataFrame:
        _, labels = connected_components(csgraph=city_graph, directed=False, return_labels=True)
        return pd.DataFrame({"driver_id": city_drivers, "group": labels})

    def __filter_gangs(self, city_gangs: pd.DataFrame) -> pd.DataFrame:
        gangs_sizes = city_gangs.groupby("group").size()
        return city_gangs[
            city_gangs.group.isin(gangs_sizes[gangs_sizes > self.thresholds['more_driver_gang_size']].index)]

    def __import_gang_sources(self) -> "tuple[pd.DataFrame, pd.DataFrame]":
        drivers_intersections, column_names = self.fraud_db.execute_query(f"""
            select * from data.{self.fraud_table_names['intersections_table']} 
            where count_samepass > {self.thresholds['more_count_samepass_edge']};""")
        drivers_intersections_df = pd.DataFrame(drivers_intersections, columns=column_names)
        drivers = drivers_intersections_df['driver_id_1'].tolist()
        drivers.extend(drivers_intersections_df['driver_id_2'].tolist())
        drivers = tuple(set(drivers))
        drivers_cities, column_names = self.fraud_db.execute_query(f"""
            select up.driver_id,c.name as city
            from data.{self.fraud_table_names['uniqpass_table']} as up
                join data.{self.fraud_table_names['cities_table']} as c
                    on c.id = up.city_id
            WHERE up.driver_id in {drivers};""")
        drivers_cities_df = pd.DataFrame(drivers_cities, columns=column_names)
        drivers_cities_df = drivers_cities_df.drop_duplicates(subset=['driver_id'])
        return drivers_intersections_df, drivers_cities_df

    def __possible_gang_cities(self, drivers_cities_df: pd.DataFrame) -> tuple:
        n_unique_drivers_per_city = drivers_cities_df.groupby('city').driver_id.nunique()
        return tuple(
            n_unique_drivers_per_city[n_unique_drivers_per_city > self.thresholds['more_unique_driv_per_city']].index)

    def __add_meta_data(self, all_gangs: pd.DataFrame) -> pd.DataFrame:
        all_gangs = all_gangs.sort_values(["group", "driver_id"]).reset_index(drop=True)
        all_gangs['group_driver'] = all_gangs.city.str.cat(all_gangs.group.astype(str))
        all_gangs.rename(columns={"group": "group_number"}, inplace=True)
        all_gangs.insert(0, "created_at", (date.today() - timedelta(days=1)))
        all_gangs.insert(2, "type", "0")
        return all_gangs

    def find_gangs(self) -> pd.DataFrame:
        drivers_intersections_df, drivers_cities_df = self.__import_gang_sources()

        if len(drivers_cities_df) > 0:
            possible_cities = self.__possible_gang_cities(drivers_cities_df)
            cities_gangs = []
            for city in possible_cities:
                city_drivers = drivers_cities_df[drivers_cities_df['city'] == city].driver_id.tolist()
                city_drivers_intersections = drivers_intersections_df.loc[
                    (drivers_intersections_df['driver_id_1'].isin(city_drivers)) |
                    (drivers_intersections_df['driver_id_2'].isin(city_drivers))]
                city_drivers, city_graph = self.__build_graph(city_drivers_intersections)
                city_gangs = self.__find_gang(city_drivers, city_graph)
                filtered_gangs = self.__filter_gangs(city_gangs)
                filtered_gangs = pd.merge(filtered_gangs, drivers_cities_df, on='driver_id')
                determined_gangs = filtered_gangs[filtered_gangs['city'] == city]
                cities_gangs.append(determined_gangs)

            all_gangs = pd.concat(cities_gangs, axis=0)
            all_gangs = self.__add_meta_data(all_gangs)

            all_gangs = all_gangs.reindex(
                columns=['driver_id', 'type', 'group_number', 'city', 'group_driver', 'created_at'])
            return all_gangs
        else:
            # Change to error or sth else
            return pd.DataFrame()
