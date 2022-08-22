import entities.User;
import orm.EntityManager;
import orm.MyConnector;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;

public class Main {
    public static void main(String[] args) throws SQLException {
        MyConnector.createConnection("root", "1234", "test_db");
        Connection connection = MyConnector.getConnection();

        EntityManager<User> userEntityManager = new EntityManager<>(connection);

        User user = new User("pesho1", "1234", 32, LocalDate.now());

        userEntityManager.persist(user);

        user.setUsername("pesho2");
        user.setPassword("123456789");
        user.setAge(33);
        user.setRegistrationDate(LocalDate.now().minusYears(2));

        userEntityManager.persist(user);
    }
}
