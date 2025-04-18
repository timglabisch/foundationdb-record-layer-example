import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Function;

public class Demo {
    public static void main(String[] args) {
        FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();

        // Define the keyspace for our application
        KeySpace keySpace = new KeySpace(new KeySpaceDirectory("record-layer-demo", KeySpaceDirectory.KeyType.STRING, "record-layer-demo"));
// Get the path where our record store will be rooted
        KeySpacePath path = keySpace.path("record-layer-demo");

        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(RecordLayerDemoProto.getDescriptor());

        metaDataBuilder.getRecordType("Order")
                .setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("order_id")));

        // metaDataBuilder.addIndex("Order", new Index("priceIndex", Key.Expressions.field("price")));


        metaDataBuilder.getRecordType("Person")
                .setPrimaryKey(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("order_id")));

        RecordMetaData recordMetaData = metaDataBuilder.build();


        recordMetaData.getRecordTypes().forEach((name, type) -> {
            System.out.println(name + " => recordTypeKey = " + type.getRecordTypeKey());
        });

        Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context -> FDBRecordStore.newBuilder()
                .setMetaDataProvider(recordMetaData)
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();

        db.run(context -> {
            Transaction tr = context.ensureActive();
            tr.clear(path.toSubspace(context).range());
            return null;
        });


        db.run(context -> {
            context.ensureActive().options().setTimeout(600000);
            FDBRecordStore recordStore = recordStoreProvider.apply(context);

            recordStore.saveRecord(RecordLayerDemoProto.Order.newBuilder()
                    .setOrderId(1)
                    .setPrice(123)
                    .setFlower(buildFlower(FlowerType.ROSE, RecordLayerDemoProto.Color.RED))
                    .build());


            recordStore.saveRecord(RecordLayerDemoProto.Person.newBuilder()
                    .setOrderId(1)
                    .setFoo("lala")
                    .build());
            return null;
        });

        FDBStoredRecord<Message> storedRecord = db.run(context -> {
            // load the record
            context.ensureActive().options().setTimeout(600000);

            return recordStoreProvider.apply(context).loadRecord(Tuple.from(recordMetaData.getRecordTypes().get("Person").getRecordTypeKey(), 1));
        });
        assert storedRecord != null;
// a record that doesn't exist is null
        FDBStoredRecord<Message> shouldNotExist = db.run(context ->
                recordStoreProvider.apply(context).loadRecord(Tuple.from(recordMetaData.getRecordTypes().get("Person").getRecordTypeKey(), 99999))
        );
        assert shouldNotExist == null;


        /*
        RecordLayerDemoProto.Order order = RecordLayerDemoProto.Order.newBuilder()
                .mergeFrom(storedRecord.getRecord())
                .build();
        System.out.println(order);

         */



        db.run(context -> {
            RecordCursor<FDBStoredRecord<Message>> cursor = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(recordMetaData)
                    .setKeySpacePath(path)
                    .open()
                    .scanRecords(null, ScanProperties.FORWARD_SCAN);

            cursor.forEach(record -> {
                System.out.println(record.getRecord());
            });
            return null;
        });


        try (FDBDatabaseRunner runner = db.newRunner()) {
            runner.run(context -> {
                byte[] begin = new byte[0];
                byte[] end = new byte[] { (byte) 0xFF };

                List<KeyValue> kvs = context.ensureActive().getRange(begin, end, 100).asList().join();

                for (KeyValue kv : kvs) {
                    System.out.println("Key:   " + printable(kv.getKey()));
                    System.out.println("Value: " + printable(kv.getValue()));
                    System.out.println("----");
                }
                return null;
            });
        }


    }

    private enum FlowerType {
        ROSE,
        TULIP,
        LILY,
    }

    private static RecordLayerDemoProto.Flower buildFlower(FlowerType type, RecordLayerDemoProto.Color color) {
        return RecordLayerDemoProto.Flower.newBuilder()
                .setType(type.name())
                .setColor(color)
                .build();
    }

    public static String printable(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            if (b >= 32 && b < 127) {
                sb.append((char) b);
            } else {
                sb.append(String.format("\\x%02X", b));
            }
        }
        return sb.toString();
    }
}
