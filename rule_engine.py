import pandas as pd
import numpy as np


def evaluate_condition(field_series:pd.Series, operand:str, value):
    if operand == 'more':
        return field_series > value
    elif operand == 'less':
        return field_series < value
    elif operand ==  'eq':
        return field_series == value
    elif operand ==  'noteq':
        return field_series != value
    elif operand ==  'moreeq':
        return field_series >= value
    elif operand ==  'lesseq':
        return field_series <= value
    else:
        raise ValueError(f"Unknown operand: {operand}")


class RuleEngine:
    def __init__(self, profile_df):
        self.profile_df = profile_df

    def apply(self, rules:dict):
        # Initialize a Series with False values
        results = pd.Series(False, index=self.profile_df.index)

        # Apply each rule to the DataFrame
        for rule_name, conditions in rules.items():
            print(rule_name)
            rule_result = self.evaluate_conditions(conditions)
            results = np.logical_or(results, rule_result)  # Logical OR to accumulate results
        return results

    def evaluate_conditions(self, conditions:dict):
        # Start with a Series of True values for all rows
        condition_result = pd.Series(True, index=self.profile_df.index)
        # Evaluate each condition and update the result
        for condition, value in conditions.items():
            operand, field = condition.split('_', 1)
            print(field)
            print(operand)
            print(value)
            field_series = self.profile_df[field]
            condition_result = np.logical_and(evaluate_condition(field_series, operand, value), condition_result)
        return condition_result
