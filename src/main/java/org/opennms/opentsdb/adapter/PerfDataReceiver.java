/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2010-2012 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2012 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.opentsdb.adapter;

import org.opennms.netmgt.rrd.tcp.PerformanceDataProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class PerfDataReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(PerfDataReceiver.class);

    private volatile static Thread m_listenerThread;

    private static String TSDB_SERVER = "192.168.30.184";

    private static String LOCATION = "hs-fulda";

    private static int TSDB_PORT = 4242;

    private static String RRD_FILE_EXTENSION = ".rrd";

    public static void main(String[] args) {
        Thread listener = null;

        Runtime.getRuntime().addShutdownHook(createShutdownHook());

        int port = 8999;

        if (args.length < 1) {
            LOGGER.error("Defaulting to port: 8999. To change, pass valid port value as first argument.");
        } else {
            port = Integer.valueOf(args[0]);
        }

        LOGGER.error("Ready to receive OpenNMS QOS Data on TCP Port:\"+String.valueOf(port)+\"...");
        try {
            listener = createListenerThread(port);
            listener.start();
            listener.join();
        } catch (Throwable t) {
            LOGGER.error(t.getLocalizedMessage() + "\n\n '{}'", t);
        }
    }

    public static Thread createShutdownHook() {
        Thread t = new Thread() {
            @Override
            public void run() {
                System.out.println("\nHave a nice day! :)");
                Runtime.getRuntime().halt(0);
            }
        };
        return t;
    }

    public static Thread createListenerThread(final int port) {
        m_listenerThread = new Thread() {
            public void run() {
                this.setName("fail");
                try {
                    ServerSocket ssocket = new ServerSocket(port);
                    ssocket.setSoTimeout(0);
                    Socket csocket = new Socket(TSDB_SERVER, TSDB_PORT);
                    LOGGER.debug("Connect to '{}'", csocket.toString());
                    while (true) {
                        try {
                            Socket socket = ssocket.accept();
                            InputStream is = socket.getInputStream();
                            PerformanceDataProtos.PerformanceDataReadings messages = PerformanceDataProtos.PerformanceDataReadings.parseFrom(is);
                            for (PerformanceDataProtos.PerformanceDataReading message : messages.getMessageList()) {
                                StringBuffer values = new StringBuffer();
                                values.append("{ ");
                                for (int i = 0; i < message.getValueCount(); i++) {
                                    if (i != 0) {
                                        values.append(", ");
                                    }
                                    values.append(message.getValue(i));
                                }
                                values.append(" }");
                                String data = message.getPath().replaceFirst(".*/", "").replaceFirst(RRD_FILE_EXTENSION, "") + " " + message.getTimestamp() / 1000 + " " + values.toString() + " " + "interface=" + message.getOwner();
                                data = data.replaceAll("\\{ ", "").replaceAll("\\} ", "");
                                PrintStream out = new PrintStream(csocket.getOutputStream());
                                LOGGER.trace("DATA: '{}", data);
                                // echo "put disc.dsUsed 1378125680 440 host=superhost" | nc -w 15 192.168.30.184 4242
                                // icmp 1378128312 { 293.0 } interface=193.174.29.23
                                out.println("put " + data + " onms_server=" + TSDB_SERVER + " opennms=true onms_location=" + LOCATION);
                                out.flush();
                            }
                        } catch (SocketTimeoutException e) {
                            LOGGER.error(e.getLocalizedMessage());
                            if (this.isInterrupted()) {
                                LOGGER.error("Interrupted");
                                this.setName("notfailed");
                                return;
                            }
                        } catch (IOException e) {
                            LOGGER.error(e.getLocalizedMessage());
                        }
                    }
                } catch (IOException e) {
                    LOGGER.error(e.getLocalizedMessage());
                } catch (Throwable e) {
                    LOGGER.error(e.getLocalizedMessage());
                }
            }
        };
        return m_listenerThread;
    }
}
