"""
This Python script contains helper functions to assist with
SageMaker Feature Store notebooks. It contains functions that
assist in creation of Feature Groups and Collection/list types.
"""
import argparse
import logging
import boto3
import time
import os
from datetime import datetime


from sagemaker.feature_store.feature_definition import (
    FeatureDefinition, 
    StringFeatureDefinition, 
    IntegralFeatureDefinition, 
    FractionalFeatureDefinition, 
    FeatureTypeEnum, 
    CollectionType, 
    CollectionTypeEnum, 
    ListCollectionType
)
from sagemaker.feature_store.inputs import (
    FeatureValue,
    FeatureParameter,
    TableFormatEnum,
    Filter,
    ResourceEnum,
    Identifier,
    DeletionModeEnum,
    TtlDuration,
    OnlineStoreConfigUpdate,
    OnlineStoreStorageTypeEnum,
)
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.feature_store.feature_store import FeatureStore


def get_order_features_with_list():

    order_features = [
        FeatureValue(feature_name='order_id', value_as_string="O999"),
        FeatureValue(feature_name='customer_id', value_as_string="C999"),
        FeatureValue(feature_name='product_id', value_as_string="P999"),
        FeatureValue(feature_name='purchase_amount', value_as_string="9.99"),
        FeatureValue(feature_name='is_reordered', value_as_string="1"),
        FeatureValue(feature_name='event_time', value_as_string="2020-10-30T03:43:21Z"),
        FeatureValue(feature_name='n_days_since_last_purchase', value_as_string="0.12209302325581398"),
        FeatureValue(feature_name='n_days_list', value_as_string_list=['13','27','7','11','22']),
    ]
    return order_features


def get_feature_definitions_with_list():
    
    feature_definition_with_collection = [
        StringFeatureDefinition(feature_name='order_id'),
        StringFeatureDefinition(feature_name='customer_id'),
        StringFeatureDefinition(feature_name='product_id'),
        FractionalFeatureDefinition(feature_name='purchase_amount'),
        IntegralFeatureDefinition(feature_name='is_reordered'), 
        StringFeatureDefinition(feature_name='event_time'),
        FractionalFeatureDefinition(feature_name='n_days_since_last_purchase'),
        StringFeatureDefinition(feature_name='n_days_list', 
                                collection_type=ListCollectionType())
    ]
    return feature_definition_with_collection


def create_order_list_fg(feature_group_name, feature_store_session, role_arn):

    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=feature_store_session)
    feature_group.feature_definitions = get_feature_definitions_with_list()
    fg = feature_group.create(
        s3_uri=False,
        record_identifier_name='order_id',
        event_time_feature_name='event_time',
        role_arn=role_arn,
        enable_online_store=True,
        online_store_storage_type=OnlineStoreStorageTypeEnum.IN_MEMORY,
    )
    return fg
