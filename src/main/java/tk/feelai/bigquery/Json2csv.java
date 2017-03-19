package tk.feelai.bigquery;

/*
 * Copyright (c) 2017 Hiroshi Matsuda.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.arnx.jsonic.JSON;
import net.arnx.jsonic.JSONEventType;
import net.arnx.jsonic.JSONException;
import net.arnx.jsonic.JSONReader;

public class Json2csv {

    public static void usage() {
        System.err.println("java tk.feelai.bigquery.Json2csv [--root node_pattern_path] [--nest N] [--client_ip_only] [--encoding ENCODING] [json1 [json2 [...]]]");
        System.err.println("  Each json file should contain an array object which has same type elements.");
        System.err.println("  Please specify --root option if you need to change root node. ex. /hits (default=/)");
        System.err.println("  Please specify --field-filter option if you need to filter fields by regex. ex. (req|ua|@timestamp) (default=.*)");
        System.err.println("  Please specify --nest option if you need to change flatten level. (default=1)");
        System.err.println("  Please specify --client-ip-only option if you need to remove previousLoadBalancerIPAddress. (default=disabled)");
        System.err.println("  Please specify --encoding option if you need to change input and output encodings. (default=UTF-8)");
    }

    public static void main(String[] args) throws IOException, JSONException {
        List<Pattern> rootPath = new ArrayList<Pattern>();
        rootPath.add(Pattern.compile(""));
        Pattern fieldFilter = Pattern.compile(".*");
        int nest = 1;
        boolean clientIPOnly = false;
        String encoding = "UTF-8";
        int index = 0;
        while (index < args.length) {
            if ("--root".equals(args[index])) {
                String[] path = args[index + 1].split("/", Integer.MIN_VALUE);
                if (path[0].length() != 0) {
                    throw new IllegalArgumentException("node_pattern_path must be specified with root slash: ex. /hits");
                }
                for (int a = 1; a < path.length; a++) {
                    rootPath.add(Pattern.compile(path[a]));
                }
                index += 2;
            } else if ("--field-filter".equals(args[index])) {
                fieldFilter = Pattern.compile(args[index + 1]);
                index += 2;
            } else if ("--nest".equals(args[index])) {
                nest = Integer.parseInt(args[index + 1]);
                index += 2;
            } else if ("--client-ip-only".equals(args[index])) {
                clientIPOnly = true;
                index += 1;
            } else if ("--encoding".equals(args[index])) {
                encoding = args[index + 1];
                index += 2;
            } else {
                break;
            }
        }
        if (index < args.length) {
            for (; index < args.length; index++) {
                String file = args[index];
                Reader in = new InputStreamReader(new FileInputStream(file), encoding);
                try {
                    PrintWriter out = new PrintWriter(file + ".csv", encoding);
                    try {
                        conv(rootPath, fieldFilter, nest, clientIPOnly, in, out);
                    } finally {
                        out.close();
                    }
                } finally {
                    in.close();
                }
            }
        } else if (System.in.available() > 0) {
            conv(rootPath, fieldFilter, nest, clientIPOnly, new InputStreamReader(System.in, encoding), new PrintWriter(new OutputStreamWriter(System.out, encoding)));
        } else {
            usage();
        }
    }

    public static void conv(List<Pattern> rootPath, Pattern fieldFilter, int nestLevel, boolean clientIPOnly, Reader in, PrintWriter out) throws IOException, JSONException {
        JSONReader reader = new JSON().getReader(in);
        JSONEventType type;
        boolean first = true;
        ArrayList<String> path = new ArrayList<String>();
        path.add("");
        int nest = 0, depth = 0;
        boolean inTarget = false;
        while ((type = reader.next()) != null) {
            switch (type) {
            case STRING:
            case NUMBER:
            case BOOLEAN:
            case NULL:
                if (inTarget && isTargetField(path, depth, fieldFilter)) {
                    if (first) {
                        first = false;
                    } else {
                        out.print(",");
                    }
                }
                break;
            default:
                break;
            }
            switch (type) {
            case COMMENT:
            case WHITESPACE:
                break;
            case NAME:
                if (path.size() == depth) {
                    path.add(reader.getString());
                } else {
                    path.set(depth, reader.getString());
                }
                break;
            case START_ARRAY:
                if (path.size() == depth) {
                    path.add("");
                } else {
                    path.set(depth, "");
                }
                break;
            case START_OBJECT:
                depth++;
                inTarget |= isInTarget(rootPath, path, depth);
                if (inTarget) {
                    nest++;
                }
                break;
            case END_ARRAY:
                path.remove(depth);
                break;
            case END_OBJECT:
                depth--;
                if (inTarget && --nest == nestLevel) {
                    out.println("");
                    first = true;
                }
                inTarget = isInTarget(rootPath, path, depth);
                break;
            case STRING:
                if (inTarget && isTargetField(path, depth, fieldFilter)) {
                    String line = reader.getString().replaceAll("[\\t\\n\\r\\\"]", " ").replaceAll(", [0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+", "");
                    out.print('"');
                    out.print(line);
                    out.print('"');
                }
                break;
            case NUMBER:
                if (inTarget && isTargetField(path, depth, fieldFilter)) {
                    out.print(reader.getNumber());
                }
                break;
            case BOOLEAN:
                if (inTarget && isTargetField(path, depth, fieldFilter)) {
                    out.print('"');
                    out.print(reader.getBoolean());
                    out.print('"');
                }
                break;
            case NULL:
                break;
            }
        }
    }

    public static boolean isInTarget(List<Pattern> rootPath, List<String> path, int depth) {
        if (depth < rootPath.size()) {
            return false;
        }
        for (int a = 1; a < rootPath.size(); a++) {
            Matcher m = rootPath.get(a).matcher(path.get(a));
            if (!m.matches()) {
                return false;
            }
        }
        return true;
    }

    public static boolean isTargetField(List<String> path, int depth, Pattern fieldFilter) {
        Matcher m = fieldFilter.matcher(path.get(depth));
        return m.matches();
    }
}
