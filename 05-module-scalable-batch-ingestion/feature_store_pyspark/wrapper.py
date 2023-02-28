# Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#   http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from abc import ABCMeta
from pyspark.ml.wrapper import JavaWrapper


class SageMakerFeatureStoreJavaWrapper(JavaWrapper):
    __metaclass__ = ABCMeta
    _wrapped_class = None

    def __init__(self):
        super(SageMakerFeatureStoreJavaWrapper, self).__init__()
        self.java_obj = None

    def _py2j(self, arg):
        if isinstance(arg, SageMakerFeatureStoreJavaWrapper):
            return arg._java_obj
        else:
            return arg

    def _new_java_obj(self, java_class, *args):
        """
        Creates a java object. We convert SageMakerJavaClass arguments
        to their java versions and then hand over to JavaWrapper
        :param java_class: Java ClassName
        :param args: constructor arguments
        :return: Java Instance
        """

        java_args = []
        for arg in args:
            java_args.append(self._py2j(arg))

        return JavaWrapper._new_java_obj(java_class, *java_args)

    def _call_java(self, name, *args):
        """
        Call a Java method in our Wrapped Class
        :param name: method name
        :param args: method arguments
        :return: java method return value converted to a python object
        """

        # call the base class method first to do the actual method calls
        # then we just need to call _from_java() to convert complex types to
        # python objects
        java_args = []
        for arg in args:
            java_args.append(self._py2j(arg))
        return super(SageMakerFeatureStoreJavaWrapper, self)._call_java(name, *java_args)
