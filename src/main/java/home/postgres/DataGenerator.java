package home.postgres;

import com.google.common.net.InetAddresses;
import de.bytefish.pgbulkinsert.row.SimpleRow;

import java.net.Inet4Address;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

final class DataGenerator {

    private int index = 0;

    void populateRow(SimpleRow row) {
        row.setTimeStamp(LoadingApp.Column.event_timestamp.ordinal(), LocalDateTime.now());
        row.setInet4Addr(LoadingApp.Column.ip_address.ordinal(), randomIpAddress());
        row.setVarChar(LoadingApp.Column.identity.ordinal(), UUID.randomUUID().toString());
        row.setVarChar(LoadingApp.Column.event_type.ordinal(), typeFor(index));
        row.setInteger(LoadingApp.Column.tracking_id.ordinal(), trackingIdFor(index));
        ++index;
    }
    private static final String[] TYPES = new String[] {"uuid", "cookie-id", "hashed-id"};
    private static String typeFor(int index) {
        return TYPES[index % TYPES.length];
    }

    private static int[] TRACKING_IDS = new int[50];
    static {
        for (int i = 1; i < 51; i++) {
            TRACKING_IDS[i-1] = i;
        }
    }
    private static int trackingIdFor(int index) {
        return TRACKING_IDS[index %  TRACKING_IDS.length];
    }

    private static final Random RANDOM = ThreadLocalRandom.current();
    private static Inet4Address randomIpAddress() {
        return InetAddresses.fromInteger(RANDOM.nextInt());
    }
}
