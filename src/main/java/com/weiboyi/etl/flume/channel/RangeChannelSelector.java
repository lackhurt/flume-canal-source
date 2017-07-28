package com.weiboyi.etl.flume.channel;


import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.AbstractChannelSelector;
import org.apache.flume.channel.MultiplexingChannelSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RangeChannelSelector extends AbstractChannelSelector {


    public static final String CONFIG_MULTIPLEX_HEADER_NAME = "header";
    public static final String DEFAULT_MULTIPLEX_HEADER =
            "flume.selector.header";
    public static final String CONFIG_PREFIX_MAPPING = "mapping.";
    public static final String CONFIG_DEFAULT_CHANNEL = "default";
    public static final String CONFIG_PREFIX_OPTIONAL = "optional";

    @SuppressWarnings("unused")
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiplexingChannelSelector.class);

    private static final List<Channel> EMPTY_LIST =
            Collections.emptyList();

    private String headerName;

    private Map<String, List<Channel>> channelMapping;
    private Map<String, List<Channel>> optionalChannels;
    private List<Channel> defaultChannels;

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
        this.headerName = context.getString(CONFIG_MULTIPLEX_HEADER_NAME,
                DEFAULT_MULTIPLEX_HEADER);

        Map<String, Channel> channelNameMap = getChannelNameMap();

        defaultChannels = getChannelListFromNames(
                context.getString(CONFIG_DEFAULT_CHANNEL), channelNameMap);

        Map<String, String> mapConfig =
                context.getSubProperties(CONFIG_PREFIX_MAPPING);

        channelMapping = new HashMap<String, List<Channel>>();

        for (String headerValue : mapConfig.keySet()) {
            List<Channel> configuredChannels = getChannelListFromNames(
                    mapConfig.get(headerValue),
                    channelNameMap);

            //This should not go to default channel(s)
            //because this seems to be a bad way to configure.
            if (configuredChannels.size() == 0) {
                throw new FlumeException("No channel configured for when "
                        + "header value is: " + headerValue);
            }

            if (channelMapping.put(headerValue, configuredChannels) != null) {
                throw new FlumeException("Selector channel configured twice");
            }
        }
        //If no mapping is configured, it is ok.
        //All events will go to the default channel(s).
        Map<String, String> optionalChannelsMapping =
                context.getSubProperties(CONFIG_PREFIX_OPTIONAL + ".");

        optionalChannels = new HashMap<String, List<Channel>>();
        for (String hdr : optionalChannelsMapping.keySet()) {
            List<Channel> confChannels = getChannelListFromNames(
                    optionalChannelsMapping.get(hdr), channelNameMap);
            if (confChannels.isEmpty()) {
                confChannels = EMPTY_LIST;
            }
            //Remove channels from optional channels, which are already
            //configured to be required channels.

            List<Channel> reqdChannels = channelMapping.get(hdr);
            //Check if there are required channels, else defaults to default channels
            if (reqdChannels == null || reqdChannels.isEmpty()) {
                reqdChannels = defaultChannels;
            }
            for (Channel c : reqdChannels) {
                if (confChannels.contains(c)) {
                    confChannels.remove(c);
                }
            }

            if (optionalChannels.put(hdr, confChannels) != null) {
                throw new FlumeException("Selector channel configured twice");
            }
        }
    }
}
