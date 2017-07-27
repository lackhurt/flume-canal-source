package com.weiboyi.etl.flume.channel;


import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.AbstractChannelSelector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RangeChannelSelector extends AbstractChannelSelector {

    public static final String CONFIG_OPTIONAL = "optional";
    List<Channel> requiredChannels = null;
    List<Channel> optionalChannels = new ArrayList<Channel>();

    /**
     * Configuration to set a subset of the channels as optional.
//     */
//    public static final String CONFIG_OPTIONAL = "optional";
//    List<Channel> requiredChannels = null;
//    List<Channel> optionalChannels = new ArrayList<Channel>();
//
//    @Override
//    public List<Channel> getRequiredChannels(Event event) {
//    /*
//     * Seems like there are lot of components within flume that do not call
//     * configure method. It is conceiveable that custom component tests too
//     * do that. So in that case, revert to old behavior.
//     */
//        if (requiredChannels == null) {
//            return getAllChannels();
//        }
//        return requiredChannels;
//    }
//
//    @Override
//    public List<Channel> getOptionalChannels(Event event) {
//        return optionalChannels;
//    }
//
//    @Override
//    public void configure(Context context) {
//        String optionalList = context.getString(CONFIG_OPTIONAL);
//        requiredChannels = new ArrayList<Channel>(getAllChannels());
//        Map<String, Channel> channelNameMap = getChannelNameMap();
//        if (optionalList != null && !optionalList.isEmpty()) {
//            for (String optional : optionalList.split("\\s+")) {
//                Channel optionalChannel = channelNameMap.get(optional);
//                requiredChannels.remove(optionalChannel);
//                if (!optionalChannels.contains(optionalChannel)) {
//                    optionalChannels.add(optionalChannel);
//                }
//            }
//        }
//    }
    @Override
    public List<Channel> getRequiredChannels(Event event) {
        return null;
    }

    @Override
    public List<Channel> getOptionalChannels(Event event) {
        return null;
    }

    @Override
    public void configure(Context context) {
        String optionalList = context.getString(CONFIG_OPTIONAL);
        requiredChannels = new ArrayList<Channel>(getAllChannels());
        Map<String, Channel> channelNameMap = getChannelNameMap();
        if (optionalList != null && !optionalList.isEmpty()) {
            for (String optional : optionalList.split("\\s+")) {
                Channel optionalChannel = channelNameMap.get(optional);
                requiredChannels.remove(optionalChannel);
                if (!optionalChannels.contains(optionalChannel)) {
                    optionalChannels.add(optionalChannel);
                }
            }
        }
    }
}
