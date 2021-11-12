package com.projectfriendrec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper class for recording every combination of users and their mutual friends from a .txt input file.
public class FriendRecMapper
    extends Mapper<LongWritable, Text, LongWritable, FriendRecordWritable>{
    
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
        // Parse the file line into a userID and its friendIDs (i.e.: "0<TAB>1,2,3 into [0, [1,2,3]]")
        if(value.toString() != null) {
            String[] line = value.toString().split("\t");
            try {
                Long userID = Long.parseLong(line[0]);
                List<Long> friendIDs = new ArrayList<Long>();

                if(line != null && line.length == 2) {
                    // For each friendID, create a record <userID, (recommendedFriend)=friendID, (mutualFriend)=-1>.
                    // The mutual friend between userID and friendID is friendID itself,
                    // so mutualFriend is -1 and should not be counted in the Reduce.
                    StringTokenizer itr = new StringTokenizer(line[1], ",");
                    while (itr.hasMoreTokens()) {
                        Long friendID = Long.parseLong(itr.nextToken());
                        friendIDs.add(friendID);
                        context.write(new LongWritable(userID), new FriendRecordWritable(friendID, -1L));
                    }

                    // Create a record for each combination of friendID:
                    // <friendID, (recommended)=nextFriendID, (mutual)=userID>
                    for (int i = 0; i < friendIDs.size(); i++) {
                        for (int j = 0; j < friendIDs.size(); j++) {
                        	if (i == j) continue;
                            context.write(new LongWritable(friendIDs.get(i)),
                                    new FriendRecordWritable((friendIDs.get(j)), userID));
                        }
                    }
                }         	
            } catch(Exception e) {
            	e.printStackTrace();
            }
        }
    }
}