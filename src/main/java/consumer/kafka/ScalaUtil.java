/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package consumer.kafka;

import java.util.List;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;


public class ScalaUtil {

  /**
   * Scala 2.10 use ClassTag to replace ClassManifest
   */
    public static <T> ClassTag<T> getClassTag(Class<T> clazz) {
      return ClassTag$.MODULE$.apply(clazz);
    }

    @SuppressWarnings("unchecked")
    public static <E> ClassTag<MessageAndMetadata<E>> getMessageAndMetadataClassTag() {
      return (ClassTag<MessageAndMetadata<E>>)(Object) getClassTag(MessageAndMetadata.class);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ClassTag<Tuple2<K, V>> getTuple2ClassTag() {
      return (ClassTag<Tuple2<K, V>>)(Object) getClassTag(Tuple2.class);
    }

    public static <T> Seq<T> toScalaSeq(List<T> list) {
        return JavaConversions.asScalaBuffer(list);
    }
}