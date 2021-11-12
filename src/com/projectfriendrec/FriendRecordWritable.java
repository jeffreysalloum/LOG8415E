package com.projectfriendrec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


// Since we need a recorded value that's not a primitive data type,
// a custom FriendRecordWritable class is implemented.
public class FriendRecordWritable implements Writable {
    public Long user;
    public Long mutualFriend;
    
    public FriendRecordWritable(Long user, Long mutualFriend) {
        this.user = user;
        this.mutualFriend = mutualFriend;
    }

     public FriendRecordWritable() {
         this(-1L, -1L);
     }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(user);
        out.writeLong(mutualFriend);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        user = in.readLong();
        mutualFriend = in.readLong();
    }

    @Override
    public String toString() {
        return " userID: "
                + Long.toString(user)
                + " mutualFriend: "
                + Long.toString(mutualFriend);
    }
}
