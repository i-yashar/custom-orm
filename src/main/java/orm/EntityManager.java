package orm;

import annotations.Column;
import annotations.Entity;
import annotations.Id;

import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class EntityManager<E> implements DBContext<E>{
    private Connection connection;

    public EntityManager(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean persist(E entity) throws SQLException {
        Field primaryKey = getId(entity.getClass());
        primaryKey.setAccessible(true);
        try {
            Object value = primaryKey.get(entity);
            if(value != null && (long) value > 0) {
                return doUpdate(entity, (long) value);
            }
        } catch (IllegalAccessException ex) {
            System.out.println(ex.getMessage());
        }


        return doInsert(entity, primaryKey);
    }

    @Override
    public Iterable<E> find(Class<E> table) {
        return null;
    }

    @Override
    public Iterable<E> find(Class<E> table, String where) {
        return null;
    }

    @Override
    public E findFirst(Class<E> table) {
        return null;
    }

    @Override
    public E findFirst(Class<E> table, String where) {
        return null;
    }

    private boolean doInsert(E entity, Field primaryKey) throws SQLException {
        String tableName = getTableName(entity.getClass());
        String tableFields = getColumnsWithoutId(entity.getClass());
        String tableValues = getColumnsValuesWithoutId(entity);

        String insertQuery = String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, tableFields, tableValues);
        boolean result = connection.prepareStatement(insertQuery).execute();

        String getIdQuery = String.format("SELECT id FROM %s ORDER BY id DESC LIMIT 1", tableName);

        ResultSet rs = connection.createStatement().executeQuery(getIdQuery);
        rs.next();
        int id = rs.getInt("id");

        try {
            primaryKey.set(entity, (long) id);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return result;
    }

    private boolean doUpdate(E entity, long primaryKey) throws SQLException {
        String tableName = getTableName(entity.getClass());
        String[] tableFields = getColumnsWithoutId(entity.getClass()).split(",");
        String[] tableValues = getColumnsValuesWithoutId(entity).split(",");

        List<String> values = new ArrayList<>();

        for (int i = 0; i < tableFields.length; i++) {
            values.add(tableFields[i] + "=" + tableValues[i]);
        }

        String updateQuery = String.format("UPDATE %s SET %s WHERE id=%d",
                tableName, String.join(",", values), primaryKey);

        return connection.prepareStatement(updateQuery).execute();
    }

    private String getColumnsValuesWithoutId(E entity) {
        List<Field> fields = Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Column.class))
                .filter(f -> !f.isAnnotationPresent(Id.class))
                .collect(Collectors.toList());

        List<String> values = new ArrayList<>();

        for (Field field : fields) {
            field.setAccessible(true);
            try {
                Object o = field.get(entity);
                if(o instanceof String || o instanceof LocalDate) {
                    values.add("'" + o + "'");
                } else {
                    values.add(o.toString());
                }
            } catch(IllegalAccessException ex) {
                System.out.println(ex.getMessage());
            }

        }

        return String.join(",", values);
    }

    private String getColumnsWithoutId(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Column.class))
                .filter(f -> !f.isAnnotationPresent(Id.class))
                .map(f -> f.getDeclaredAnnotation(Column.class).name())
                .collect(Collectors.joining(","));
    }

    private String getTableName(Class<?> clazz) {
        Entity entity = clazz.getDeclaredAnnotation(Entity.class);

        if(entity == null) {
            throw new UnsupportedOperationException("Class must be Entity");
        }

        return entity.name();
    }

    private Field getId(Class<?> entity) {
        return Arrays.stream(entity.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .findFirst()
                .orElseThrow(() -> new UnsupportedOperationException("Entity does not have primary key"));
    }
}
