package websocket.echo.iot.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingDeque;

import org.apache.log4j.Logger;

import com.oxid.util.DB;
import com.oxid.util.DBResource;

import websocket.echo.iot.dao.EventDao;
import websocket.echo.iot.dao.IoTBaseDao;
import websocket.echo.iot.model.BaseEvent;
import websocket.echo.iot.model.HTTPEvent;
import websocket.echo.iot.model.HostSecurity;
import websocket.echo.iot.model.TCPEvent;
import websocket.echo.iot.model.UDPEvent;

public class EventDaoImpl<T extends BaseEvent> extends IoTBaseDao implements EventDao<T> {
    static final Logger logger = Logger.getLogger(EventDaoImpl.class);

    /*
    public static void add(BaseEvent event) {
    }
    
    public static void add(TCPEvent event) {
        boolean executed = db
                .easilyExecutePreparedStmt(
                        "INSERT INTO tcp_event (vehicle_id, date, dest_ip, dest_port, security) VALUES (?, ?, ?, ?, ?)",
                        new Object[] { event.vehicleId, event.date,
                                event.destIP, event.destPort, "" + event.security});
        if (!executed) {
            logger.error("lost db connection?");
        }
    }
    
    public static void add(UDPEvent event) {
        boolean executed = db
                .easilyExecutePreparedStmt(
                        "INSERT INTO udp_event (vehicle_id, date, dest_ip, dest_port, security) VALUES (?, ?, ?, ?, ?)",
                        new Object[] { event.vehicleId, event.date,
                                event.destIP, event.destPort, "" + event.security});
        if (!executed) {
            logger.error("lost db connection?");
        }
    }
    
    public static void add(HTTPEvent event) {
        boolean executed = db
                .easilyExecutePreparedStmt(
                        "INSERT INTO http_event (vehicle_id, date, dest_ip, dest_port, method, host, security, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        new Object[] { event.vehicleId, event.date,
                                event.destIP, event.destPort, event.method,
                                event.host, "" + event.security, event.url });
        if (!executed) {
            logger.error("lost db connection?");
        }
    }
*/
    
    public static List<HTTPEvent> searchHTTPEvents(int vehicle_id, Date start, Date end) {
        DBResource resource = db
                .executePreparedStmt(
                        "SELECT date, dest_ip, dest_port, method, host, security, url FROM event WHERE vehicle_id = ? AND date >= ? AND date <= ?",
                        new Object[] { vehicle_id, start, end });
        if (resource == null) {
            logger.error("lost db connection?");
            return null;
        }
        List<HTTPEvent> res = new ArrayList<>();
        ResultSet rs = resource.rs;
        try {
            while (rs.next()) {
                res.add(new HTTPEvent(vehicle_id, ZonedDateTime.ofInstant(rs.getTimestamp(1).toInstant(), ZoneId.systemDefault()),
                        HostSecurity.valueOf(rs.getString(6)), rs.getString(2),
                        rs.getInt(3), rs.getString(4), rs.getString(5), rs
                                .getString(7)));
            }
        } catch (SQLException e) {
            logger.error("vehicle_id = " + vehicle_id, e);
        } finally {
            DB.close(resource);
        }
        return res;
    }
    
    public static List<HTTPEvent> searchAllHTTPEvents(int vehicle_id) {
        DBResource resource = db
                .executePreparedStmt(
                        "SELECT date, dest_ip, dest_port, method, host, security, url FROM http_event WHERE vehicle_id = ?",
                        new Object[] { vehicle_id });
        if (resource == null) {
            logger.error("lost db connection?");
            return null;
        }
        List<HTTPEvent> res = new ArrayList<>();
        ResultSet rs = resource.rs;
        try {
            while (rs.next()) {
                res.add(new HTTPEvent(vehicle_id, ZonedDateTime.ofInstant(rs.getTimestamp(1).toInstant(), ZoneId.systemDefault()),
                		HostSecurity.valueOf(Integer.parseInt(rs.getString(6))), rs.getString(2),
                        rs.getInt(3), rs.getString(4), rs.getString(5), rs
                                .getString(7)));
            }
        } catch (SQLException e) {
            logger.error("vehicle_id = " + vehicle_id, e);
        } finally {
            DB.close(resource);
        }
        return res;
    }
    
    public static List<HTTPEvent> searchCurrentHTTPEvents(int vehicle_id, Date start) {
        DBResource resource = db
                .executePreparedStmt(
                        "SELECT date, dest_ip, dest_port, method, host, security, url FROM http_event WHERE vehicle_id = ? AND date >= ?",
                        new Object[] { vehicle_id, start });
        if (resource == null) {
            logger.error("lost db connection?");
            return null;
        }
        List<HTTPEvent> res = new ArrayList<>();
        ResultSet rs = resource.rs;
        try {
            while (rs.next()) {
                res.add(new HTTPEvent(vehicle_id, ZonedDateTime.ofInstant(rs.getTimestamp(1).toInstant(), ZoneId.systemDefault()),
                        HostSecurity.valueOf(Integer.parseInt(rs.getString(6))), rs.getString(2),
                        rs.getInt(3), rs.getString(4), rs.getString(5), rs
                                .getString(7)));
            }
        } catch (SQLException e) {
            logger.error("vehicle_id = " + vehicle_id, e);
        } finally {
            DB.close(resource);
        }
        return res;
    }
    
    
    // ---- add -----
    /**
     * @author aaron
     * */    
    public static Map<Integer, List<String>> getQbotRuleInfoFromDB() {
		Map<Integer, List<String>> map = new HashMap<>();
	    	DBResource resource = websocket.echo.iot.dao.IoTBaseDao.db
	        .executePreparedStmt(
	        		"SELECT rule_id, name, pattern, type, protocol" 
	        		+ "FROM qbot_rules",
	        		new Object[] {});   	
		if (resource == null) {
			logger.error("lost db connection?");
			return null;
		}
		ResultSet rs = resource.rs;
		try {			
			while (rs.next()) {
				List<String> list = new ArrayList<>();
				list.add(rs.getString(2));
				list.add(rs.getString(3));
				list.add(rs.getString(4));
				list.add(rs.getString(5));
				map.put(rs.getInt(1), list );
			}
		} catch (SQLException e) {
			List<String> list = new ArrayList<>();
			list.add(e.toString());
			logger.error("exception in qbot_rules table: " + e);
		} finally {
			DB.close(resource);
		}
				
		return map;	
    }
    

    
    // ---- add -----

    
    public static Set<String> getHttpBlacklistInfoFromDB() {
    		Set<String> set = new HashSet<>();
	    	DBResource resource = websocket.echo.iot.dao.IoTBaseDao.db
	        .executePreparedStmt(
	        			"SELECT host FROM http_blacklist",
	                new Object[] {});
	    		    	
		if (resource == null) {
			logger.error("lost db connection?");	
			return null;
		}
		
		ResultSet rs = resource.rs;
		try {			
			while (rs.next()) {
				set.add(rs.getString(1));
			}
		} catch (SQLException e) {
			set.add(e.toString());
		} finally {
			DB.close(resource);
		}
		return set;	
	}
    
    
    public static Set<String> getTcpBlacklistInfoFromDB() {
		Set<String> set = new HashSet<>();
	    	DBResource resource = websocket.echo.iot.dao.IoTBaseDao.db
	        .executePreparedStmt(
	        			"SELECT ip FROM tcp_blacklist",
	                new Object[] {});
		if (resource == null) {
			logger.error("lost db connection?");	
			return null;
		}
		ResultSet rs = resource.rs;
		try {			
			while (rs.next()) {
				set.add(rs.getString(1));
			}
		} catch (SQLException e) {
			set.add(e.toString());
		} finally {
			DB.close(resource);
		}
		return set;	
	}
    
    
    public static Set<String> getUdpBlacklistInfoFromDB() {
		Set<String> set = new HashSet<>();
	    	DBResource resource = websocket.echo.iot.dao.IoTBaseDao.db
	        .executePreparedStmt(
	        			"SELECT ip FROM udp_blacklist",
	                new Object[] {});
		if (resource == null) {
			logger.error("lost db connection?");	
			return null;
		}
		ResultSet rs = resource.rs;
		try {			
			while (rs.next()) {
				set.add(rs.getString(1));
			}
		} catch (SQLException e) {
			set.add(e.toString());
		} finally {
			DB.close(resource);
		}
		return set;	
	}
    
    // ----------
    public static List<UDPEvent> searchAlUDPEvents(int vehicle_id) {
        DBResource resource = db
                .executePreparedStmt(
                        "SELECT date, dest_ip, dest_port, security FROM udp_event WHERE vehicle_id = ?",
                        new Object[] { vehicle_id });
        if (resource == null) {
            logger.error("lost db connection?");
            return null;
        }
        List<UDPEvent> res = new ArrayList<>();
        ResultSet rs = resource.rs;
        try {
            while (rs.next()) {
                res.add(new UDPEvent(vehicle_id, ZonedDateTime.ofInstant(rs.getTimestamp(1).toInstant(), ZoneId.systemDefault()),
                		HostSecurity.valueOf(Integer.parseInt(rs.getString(4))), rs.getString(2),
                        rs.getInt(3)));
            }
        } catch (SQLException e) {
            logger.error("vehicle_id = " + vehicle_id, e);
        } finally {
            DB.close(resource);
        }
        return res;
    }
    
    public static List<UDPEvent> searchCurrentUDPEvents(int vehicle_id, Date start) {
        DBResource resource = db
                .executePreparedStmt(
                        "SELECT date, dest_ip, dest_port, security FROM udp_event WHERE vehicle_id = ? AND date >= ?",
                        new Object[] { vehicle_id, start });
        if (resource == null) {
            logger.error("lost db connection?");
            return null;
        }
        List<UDPEvent> res = new ArrayList<>();
        ResultSet rs = resource.rs;
        try {
            while (rs.next()) {
                res.add(new UDPEvent(vehicle_id, ZonedDateTime.ofInstant(rs.getTimestamp(1).toInstant(), ZoneId.systemDefault()),
                		HostSecurity.valueOf(Integer.parseInt(rs.getString(4))), rs.getString(2),
                        rs.getInt(3)));
            }
        } catch (SQLException e) {
            logger.error("vehicle_id = " + vehicle_id, e);
        } finally {
            DB.close(resource);
        }
        return res;
    }
    
    
    
    public static List<TCPEvent> searchAllTCPEvents(int vehicle_id) {
        DBResource resource = db
                .executePreparedStmt(
                        "SELECT date, dest_ip, dest_port, security, payload, qbot_rule_id FROM tcp_event WHERE vehicle_id = ?",
                        new Object[] { vehicle_id });
        if (resource == null) {
            logger.error("lost db connection?");
            return null;
        }
        List<TCPEvent> res = new ArrayList<>();
        ResultSet rs = resource.rs;
        try {
            while (rs.next()) {
                res.add(new TCPEvent(vehicle_id, ZonedDateTime.ofInstant(rs.getTimestamp(1).toInstant(), ZoneId.systemDefault()),
                		HostSecurity.valueOf(Integer.parseInt(rs.getString(4))), rs.getString(2),
                        rs.getInt(3)));
            }
        } catch (SQLException e) {
            logger.error("vehicle_id = " + vehicle_id, e);
        } finally {
            DB.close(resource);
        }
        return res;
    }
    
    
    public static List<TCPEvent> searchCurrentTCPEvents(int vehicle_id, Date start) {
        DBResource resource = db
                .executePreparedStmt(
                        "SELECT date, dest_ip, dest_port, security, payload, qbot_rule_id FROM tcp_event WHERE vehicle_id = ? AND date >= ?",
                        new Object[] { vehicle_id, start });
        if (resource == null) {
            logger.error("lost db connection?");
            return null;
        }
        List<TCPEvent> res = new ArrayList<>();
        ResultSet rs = resource.rs;
        try {
            while (rs.next()) {
                res.add(new TCPEvent(vehicle_id, ZonedDateTime.ofInstant(rs.getTimestamp(1).toInstant(), ZoneId.systemDefault()),
                		HostSecurity.valueOf(Integer.parseInt(rs.getString(4))), rs.getString(2),
                        rs.getInt(3)));
            }
        } catch (SQLException e) {
            logger.error("vehicle_id = " + vehicle_id, e);
        } finally {
            DB.close(resource);
        }
        return res;
    }
    
    @SuppressWarnings("unchecked")
    public boolean easilyBatchInsert(BlockingDeque<T> list, int batchSize) {
        if (list.size() == 0) {
            return true;
        }
        T t = list.getFirst();
        if (t instanceof TCPEvent) {
            return insertTCP((BlockingDeque<TCPEvent>) list, batchSize);
        } else if (t instanceof UDPEvent) {
            return insertUDP((BlockingDeque<UDPEvent>) list, batchSize);
        } else if (t instanceof HTTPEvent) {
            return insertHTTP((BlockingDeque<HTTPEvent>) list, batchSize);
        }
        return false;
    }

    private boolean insertTCP(BlockingDeque<TCPEvent> list, int batchSize) {
        int size = list.size();
        System.out.println("tcp list.size() = " + size + ", batchSize = "
                + batchSize + ", preparing params: "
                + System.currentTimeMillis());
        String sql = "INSERT INTO tcp_event (vehicle_id, date, dest_ip, dest_port, security, payload, qbot_rule_id) VALUES ((?), (?), (?), (?), (?), (?), (?))";
        Object[][] params = new Object[size][7];
        int i = 0;
        while (!list.isEmpty() && --size > -1) {
            try {
                TCPEvent event = list.take();
                if (event != null) {
                    params[i][0] = event.getVehicleId();
                    params[i][1] = event.getDate().toLocalDateTime();
                    params[i][2] = event.getDestIP();
                    params[i][3] = event.getDestPort();
                    params[i][4] = event.getSecurity().idx;
                    params[i][5] = event.getPayload();
                    params[i][6] = event.getQbotRuleId();
                    ++i;
                }
            } catch (Exception e) {
                logger.error("", e);
                System.out.println("list.size() = " + list.size());
            }
        }
        if (i != params.length) {
            System.err.println("i = " + i + ", tcp params.length = "
                    + params.length);
        }
        System.out.println("i = " + i + ", prepared tcp params: "
                + System.currentTimeMillis());
        return db.easilyBatchInsert(sql, params, batchSize);
    }

    private boolean insertUDP(BlockingDeque<UDPEvent> list, int batchSize) {
        int size = list.size();
        System.out.println("udp list.size() = " + size + ", preparing params: "
                + System.currentTimeMillis());
        String sql = "INSERT INTO udp_event (vehicle_id, date, dest_ip, dest_port, security) VALUES (?, ?, ?, ?, ?)";
        Object[][] params = new Object[size][5];
        int i = 0;
        while (!list.isEmpty() && --size > -1) {
            try {
                UDPEvent event = list.take();
                if (event != null) {
                    params[i][0] = event.getVehicleId();
                    params[i][1] = event.getDate().toLocalDateTime();
                    params[i][2] = event.getDestIP();
                    params[i][3] = event.getDestPort();
                    params[i][4] = event.getSecurity().idx;
                    ++i;
                }
            } catch (Exception e) {
                logger.error("", e);
                System.out.println("list.size() = " + list.size());
            }
        }
        if (i != params.length) {
            System.err.println("i = " + i + ", udp params.length = "
                    + params.length);
        }
        System.out
                .println("prepared udp params: " + System.currentTimeMillis());
        return db.easilyBatchInsert(sql, params, batchSize);
    }

    private boolean insertHTTP(BlockingDeque<HTTPEvent> list, int batchSize) {
        int size = list.size();
        System.out.println("http list.size() = " + size
                + ", preparing params: " + System.currentTimeMillis());
        String sql = "INSERT INTO http_event (vehicle_id, date, dest_ip, dest_port, method, host, security, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        Object[][] params = new Object[size][8];
        int i = 0;
        while (!list.isEmpty() && --size > -1) {
            try {
                HTTPEvent event = list.take();
                if (event != null) {
                    params[i][0] = event.getVehicleId();
                    params[i][1] = event.getDate().toLocalDateTime();
                    params[i][2] = event.getDestIP();
                    params[i][3] = event.getDestPort();
                    params[i][4] = event.getMethod();
                    params[i][5] = event.getHost();
                    params[i][6] = event.getSecurity().idx;
                    params[i][7] = event.getUrl();
                    ++i;
                }
            } catch (Exception e) {
                logger.error("", e);
                System.out.println("list.size() = " + list.size());
            }
        }
        if (i != params.length) {
            System.err.println("i = " + i + ", http params.length = "
                    + params.length);
        }
        System.out.println("prepared http params: "
                + System.currentTimeMillis());
        return db.easilyBatchInsert(sql, params, batchSize);
    }
    /*
    public boolean easilyBatchInsert2(List<TCPEvent> list, int batchSize) {
        if(list.size() == 0) {
            return true;
        }
        System.out.println("preparing params: " + System.currentTimeMillis());
        String sql = "INSERT INTO tcp_event (vehicle_id, date, dest_ip, dest_port, security) VALUES (?, ?, ?, ?, ?)";
        Object[][] params = new Object[list.size()][5];
        int i = 0;
        for (TCPEvent event : list) {
            params[i][0] = event.getVehicleId();
            params[i][1] = event.getDate();
            params[i][2] = event.getDestIP();
            params[i][3] = event.getDestPort();
            params[i][4] = event.getSecurity().idx;
            ++i;
        }
        System.out.println("prepared params: " + System.currentTimeMillis());
        Connection con = db.getCon();
        return db.easilyBatchInsert(con, sql, params, batchSize);
    }
    */
    
    public void testJDBC() {
        db.easilyExecutePreparedStmt(
                        "INSERT INTO udp_whitelist VALUES (?, ?, ?)",
                        new Object[] { new Random().nextInt(Integer.MAX_VALUE), "1.2.3.4", new Random().nextInt(Integer.MAX_VALUE) });
    }
    
    public void testJDBC2() {
        db.easilyExecutePreparedStmt(
                        "INSERT INTO tcp_event VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        new Object[] { new Random().nextInt(Integer.MAX_VALUE), 1124019, LocalDateTime.now(), "1.2.3.4",
                                9999, 0, null, null});
    }
}