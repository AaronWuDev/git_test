package websocket.echo.iot.dao.impl;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.hibernate.query.NativeQuery;
import org.hibernate.query.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.exception.ConstraintViolationException;

import com.oxid.persistence.HibernateUtil;

import org.hibernate.exception.DataException;
import org.hibernate.ObjectNotFoundException;

import websocket.echo.iot.dao.BaseDao;

public class BaseDaoImpl implements BaseDao {
    static Logger logger = Logger.getLogger(BaseDaoImpl.class);

    /**
     * get an object from database using given primary key
     * 
     * @param c
     * @param pk
     * @return
     */
    public Object get(Class<?> c, Serializable pk) {
        Object o = null;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            o = session.get(c, pk);
        } catch (Exception e) {
            logger.error("pk = " + pk, e);
        } finally {
            
            close(tx, session);
        }
        return o;
    }

    /**
     * find all records from DB satisfying with given hql and parameters
     */
    @SuppressWarnings("rawtypes")
    public List findAll(String hql, Object... params) {
        List list = null;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            Query query = session.createQuery(hql);
            for (int i = 0; i < params.length; i++) {
                query.setParameter(i + 1, params[i]);
                logger.debug("setParameter(" + i + ")");
            }
            list = query.list();
        } catch (Exception e) {
            if (tx != null)
                tx.rollback();
            logger.error("hql = " + hql, e);
        } finally {
            close(tx, session);
        }
        return list;
    }

    /**
     * find all records from DB satisfying with given hql and parameters with a
     * obscure select
     */
    @SuppressWarnings("rawtypes")
    public List findAlllike(String hql, Object... params) {
        List list = null;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            Query query = session.createQuery(hql);
            for (int i = 0; i < params.length; i++) {
                query.setParameter(i, params[i]);
                logger.debug("setParameter(" + i + ")");
            }
            list = query.list();
        } catch (Exception e) {
            if (tx != null)
                tx.rollback();
            logger.error("hql = " + hql, e);
            logger.error(e);
        } finally {
            close(tx, session);
        }
        return list;
    }

    /**
     * find the first record from DB satisfying with given hql and parameters
     * 
     * @param hql
     * @param params
     * @return
     */
    @SuppressWarnings("rawtypes")
    public Object findFirst(String hql, Object... params) {
        List list = findAll(hql, params);
        return (list == null || list.size() == 0) ? null : list.get(0);
    }

    @SuppressWarnings("unchecked")
    public Integer findMaxURL_id() {
        List<Map<String, Integer>> ids = new LinkedList<>();
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            NativeQuery<Map<String, Integer>> query = session
                    .createNativeQuery("SELECT max(URL_id) as max_id from URL");
            //query.setResultTransformer(Criteria.ALIAS_TO_ENTITY_MAP);
            ids = query.list();
            logger.info("ids = " + ids);
        } catch (Exception e) {
            logger.error(e);
        } finally {
            close(tx, session);
        }
        Integer max_id = ids.get(0).get("max_id");
        return max_id == null ? 0 : max_id;
    }

    public Serializable save(Object o) {
        Serializable generatedId = null;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            generatedId = session.save(o);
        } catch (ConstraintViolationException e) {
            logger.error(e);
        } catch (DataException e) {
            logger.info("o = " + o);
            logger.error(e);
        } catch (Exception e) {
            logger.info("o = " + o);
            logger.error(e);
        } finally {
            close(tx, session);
        }
        return generatedId;
    }

    public void update(Object o) {
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            session.update(o);
        } catch (Exception e) {
            logger.error(e);
        } finally {
            close(tx, session);
        }
    }

    @SuppressWarnings("unchecked")
    public List<Object[]> select(String sql, Object... params) {
        List<Object[]> list = null;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            NativeQuery<Object[]> query = session.createNativeQuery(sql);
            for (int i = 0; i < params.length; i++) {
                logger.info("params[" + i + "] = " + params[i]);
                query.setParameter(i, params[i]);
            }
            list = query.list();
        } catch (Exception e) {
            logger.error(e);
        } finally {
            close(tx, session);
        }
        return list;
    }

    public int update(String sql, Object... params) {
        int count = -1;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            NativeQuery<?> query = session.createNativeQuery(sql);
            for (int i = 0; i < params.length; i++) {
                logger.info("params[" + i + "] = " + params[i]);
                query.setParameter(i, params[i]);
            }
            count = query.executeUpdate();
        } catch (Exception e) {
            logger.error(e);
        } finally {
            close(tx, session);
        }
        return count;
    }

    public void saveOrUpdate(Object o) throws ConstraintViolationException {
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            // logger.info("start saveOrUpdate : " + o);
            session.saveOrUpdate(o);
            // logger.info("finish saveOrUpdate : " + o);
        } catch (Exception e) {
            logger.error(e);
        } finally {
            close(tx, session);
        }
    }

    protected void close(Transaction tx, Session session) {
        if (tx != null) {
            tx.commit();
        }
        if (session != null) {
            session.close();
        }
    }

    public boolean deleteById(Class<?> type, Serializable id) {
        boolean successful = false;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            Object persistentInstance = session.load(type, id);
            // logger.info("persistentInstance = " + persistentInstance);
            if (persistentInstance != null) {
                session.delete(persistentInstance);
                successful = true;
            }
        } catch (ObjectNotFoundException e) {
            logger.warn(e);
        } catch (Exception e) {
            logger.error(e);
        } finally {
            close(tx, session);
        }
        return successful;
    }

    /**
     * find the next AUTO_INCREMENT id from given table
     * 
     * @param tableName
     * @return the next AUTO_INCREMENT id from given table
     */
    public Integer getNextId(String tableName) {
        return getInt(
                "SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'meta_db' AND TABLE_NAME = ?",
                tableName);
    }

    /**
     * find an int using given sql and params
     * 
     * @param sql
     * @param params
     * @return an int using given sql and params
     */
    public Integer getInt(String sql, String... params) {
        Integer res = -1;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            NativeQuery<?> query = session.createNativeQuery(sql);
            for (int i = 0; i < params.length; i++) {
                query.setParameter(i, params[i]);
            }
            res = Integer.parseInt(query.list().get(0).toString());
        } catch (Exception e) {
            logger.error("sql = " + sql, e);
        } finally {
            close(tx, session);
        }
        return res;
    }

    /**
     * find first object satisfying certain requirements in hql
     * 
     * @param hql
     * @return first object satisfying certain requirements in hql
     */
    public Object findFirst(String hql) {
        @SuppressWarnings("rawtypes")
        List list = null;
        Session session = HibernateUtil.getSession();
        Transaction tx = session.beginTransaction();
        try {
            Query<?> query = session.createQuery(hql);
            query.setFirstResult(0);
            query.setMaxResults(1);
            list = query.list();
        } catch (Exception e) {
            logger.error("hql = " + hql, e);
        } finally {
            close(tx, session);
        }
        return (list == null || list.size() == 0) ? null : list.get(0);
    }
}