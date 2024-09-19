import numpy as np
import pandas as pd
import datetime
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components

from datetime import date, timedelta, datetime
import logging
from query_handler import SQLDatabaseHandler


class Passenger_gang():

    def __init__(self, fraud_db: SQLDatabaseHandler, fraud_table_names: dict, thresholds: dict):
        self.fraud_table_names = fraud_table_names
        self.fraud_db = fraud_db
        self.thresholds = thresholds

    def __build_graph(self, city_passengers_intersection: pd.DataFrame) -> "tuple[tuple, csr_matrix]":
        city_passengers_intersection = city_passengers_intersection.sort_values(by=["passenger_id_1", "passenger_id_2"])
        city_passengers_intersection.reset_index(inplace=True, drop=True)

        passengers_1 = city_passengers_intersection['passenger_id_1'].tolist()
        passengers_2 = city_passengers_intersection['passenger_id_2'].tolist()
        unique_passengers = tuple(set(passengers_1 + passengers_2))

        city_passengers_intersection['index_1'] = city_passengers_intersection['passenger_id_1'].apply(
            lambda x: unique_passengers.index(x))
        city_passengers_intersection['index_2'] = city_passengers_intersection['passenger_id_2'].apply(
            lambda x: unique_passengers.index(x))

        city_passengers_intersection = city_passengers_intersection[
            ['passenger_id_1', 'passenger_id_2', 'count_samedriv', 'index_1', 'index_2']]

        graph = csr_matrix((np.ones((len(city_passengers_intersection))),
                            (city_passengers_intersection['index_1'], city_passengers_intersection['index_2'])),
                           shape=(len(unique_passengers), len(unique_passengers)))
        return unique_passengers, graph

    def __find_gang(self, city_passengers: tuple, city_graph: csr_matrix) -> pd.DataFrame:
        _, labels = connected_components(csgraph=city_graph, directed=False, return_labels=True)
        return pd.DataFrame({"passenger_id": city_passengers, "group": labels})

    def __filter_gangs(self, city_gangs: pd.DataFrame) -> pd.DataFrame:
        gangs_sizes = city_gangs.groupby("group").size()
        return city_gangs[
            city_gangs.group.isin(gangs_sizes[gangs_sizes > self.thresholds['more_passenger_gang_size']].index)]

    def __import_gang_sources(self) -> "tuple[pd.DataFrame, pd.DataFrame]":
        passengers_intersections, column_names = self.fraud_db.execute_query(f"""
            select * from data.{self.fraud_table_names['intersections_passengers_table']} 
            where count_samedriv > {self.thresholds['more_count_samedriv_edge']};""")
        passengers_intersections_df = pd.DataFrame(passengers_intersections, columns=column_names)
        passengers = passengers_intersections_df['passenger_id_1'].tolist()
        passengers.extend(passengers_intersections_df['passenger_id_2'].tolist())
        passengers = tuple(set(passengers))
        passengers_cities, column_names = self.fraud_db.execute_query(f"""
            select su.passenger_id,c.name as city
            from data.{self.fraud_table_names['suspect_passengers_table']} as su
                join data.{self.fraud_table_names['cities_table']} as c
                    on c.id = su.city_id
            WHERE su.passenger_id in {passengers};""")
        passengers_cities_df = pd.DataFrame(passengers_cities, columns=column_names)
        passengers_cities_df = passengers_cities_df.drop_duplicates(subset=['passenger_id'])
        return passengers_intersections_df, passengers_cities_df

    def __possible_gang_cities(self, passengers_cities_df: pd.DataFrame) -> tuple:
        n_unique_passengers_per_city = passengers_cities_df.groupby('city').passenger_id.nunique()
        return tuple(n_unique_passengers_per_city[
                         n_unique_passengers_per_city >= self.thresholds['more_unique_pass_per_city']].index)

    def __add_meta_data(self, all_gangs: pd.DataFrame) -> pd.DataFrame:
        all_gangs = all_gangs.sort_values(["group", "passenger_id"]).reset_index(drop=True)
        all_gangs['group_passenger'] = all_gangs.city.str.cat(all_gangs.group.astype(str))
        all_gangs.rename(columns={"group": "group_number"}, inplace=True)
        all_gangs.insert(0, "created_at", (date.today() - timedelta(days=1)))
        all_gangs.insert(2, "type", "0")
        return all_gangs

    def find_gangs(self) -> pd.DataFrame:
        passengers_intersections_df, passengers_cities_df = self.__import_gang_sources()

        if len(passengers_cities_df) > 0:
            possible_cities = self.__possible_gang_cities(passengers_cities_df)
            cities_gangs = []
            for city in possible_cities:
                city_passengers = passengers_cities_df[passengers_cities_df['city'] == city].passenger_id.tolist()
                city_passengers_intersections = passengers_intersections_df.loc[
                    (passengers_intersections_df['passenger_id_1'].isin(city_passengers)) |
                    (passengers_intersections_df['passenger_id_2'].isin(city_passengers))]
                city_passengers, city_graph = self.__build_graph(city_passengers_intersections)
                city_gangs = self.__find_gang(city_passengers, city_graph)
                filtered_gangs = self.__filter_gangs(city_gangs)
                filtered_gangs = pd.merge(filtered_gangs, passengers_cities_df, on='passenger_id')
                determined_gangs = filtered_gangs[filtered_gangs['city'] == city]
                cities_gangs.append(determined_gangs)

            all_gangs = pd.concat(cities_gangs, axis=0)
            all_gangs = self.__add_meta_data(all_gangs)

            all_gangs = all_gangs.reindex(
                columns=['passenger_id', 'type', 'group_number', 'city', 'group_passenger', 'created_at'])
            return all_gangs

        else:
            # Change to error or sth else
            return pd.DataFrame()
