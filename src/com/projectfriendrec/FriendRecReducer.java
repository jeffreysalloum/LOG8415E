package com.projectfriendrec;

import java.io.IOException;
import java.lang.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Reducer class for collecting every mutual friends.
public class FriendRecReducer
    extends Reducer<LongWritable, FriendRecordWritable, LongWritable, Text> {
	
    // Returns a SortedMap for the friend recommendation results.
    public static Map<Long, List<Long>> sortFriendRecs(Map<Long, List<Long>> hm)
    {
        List<Map.Entry<Long, List<Long>> > list =
               new ArrayList<Map.Entry<Long, List<Long>> >(hm.entrySet());
 
        // Sort the list: 
        // Favors i1 if its list of mutual friends is larger than that of key2.
        // Favors i1 if both lists are equal in size, and key1's numerical value is smaller than key2's.
        // Favors i2 otherwise.
        list.sort(
        	 (Map.Entry<Long, List<Long>> i1,  Map.Entry<Long, List<Long>> i2) -> {
        	     Long k1 = i1.getKey();
        	     Long k2 = i2.getKey();
        		 Integer s1 = (i1.getValue() == null)? 0 : i1.getValue().size();
        		 Integer s2 = (i2.getValue() == null)? 0 : i2.getValue().size();
        		 if (s1 > s2 || (s1.equals(s2) && k1 < k2)) {
        		     return -1;                              
        		 } else {                                    
        		     return 1;                               
        		 }
        	 }
        );
        
        // Fill the results, sorted per entry.
        Map<Long, List<Long>> sortedMap = new LinkedHashMap<Long, List<Long>>();
        for (Map.Entry<Long, List<Long>> sortedListEntry : list) {
        	sortedMap.put(sortedListEntry.getKey(), sortedListEntry.getValue());
        }
        
        return sortedMap;
    }
    
    public void reduce(LongWritable key, Iterable<FriendRecordWritable> values, Context context)
        throws IOException, InterruptedException {

        // key: recommended friend; value: list of mutual friends.
    	Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();
        
        for (FriendRecordWritable val : values) {
            final Long friendID = val.user;
            final Long mutualFriend = val.mutualFriend;
            final Boolean isAlreadyFriend = (mutualFriend == -1);
            
            // Add every mutual friend of the currently iterated user into the proper list of mutual friends.
            if (isAlreadyFriend) {
            	mutualFriends.put(friendID, null);
            } else {
            	if (mutualFriends.containsKey(friendID)) { 
            		if (mutualFriends.get(friendID) != null) 
                        mutualFriends.get(friendID).add(mutualFriend);
            	} else {
                    mutualFriends.put(friendID,
             				  new ArrayList<Long>() {{ add(mutualFriend); }}
                    );
            	}
            }
        }
            
        // Sort the collected mutual friends.
        Map<Long, List<Long>> sortedMutualFriends = sortFriendRecs(mutualFriends);
        
        Integer i = 0;
        StringBuilder output = new StringBuilder();
        for (Map.Entry<Long, List<Long>> entry : sortedMutualFriends.entrySet()) {
        	if (i == 10) break;
        	
        	// Avoid outputting entries of friendIDs that are already friends with the userID.
        	if (entry.getValue() != null) {
        		output.append(entry.getKey().toString() + " (" + entry.getValue() + ")");
        	} else {
        		// The following entries should be null.
        		break;
        	}
        	output.append(",");
        	i++;
        }
        String outputStr = output.toString();
        if (outputStr.endsWith(",")) {
        	outputStr = outputStr.substring(0, output.length() - 1);
        }
        context.write(key, new Text(outputStr));
    }
}