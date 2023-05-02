package com.wherobots.snowflake.snowsql;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;


public class SnowTestRunner extends BlockJUnit4ClassRunner {

    private TestBase testObject;

    public SnowTestRunner(Class<TestBase> testClass) throws InitializationError, InstantiationException, IllegalAccessException, InvocationTargetException, SQLException {
        super(testClass);
        testObject = (TestBase) this.getTestClass().getOnlyConstructor().newInstance();
        testObject.init();
    }

    @Override
    protected Object createTest() {
        return testObject;
    }

    @Override
    public void run(RunNotifier notifier) {
        super.run(notifier);
        System.out.println("Closing SnowClient");
        testObject.tearDown();
    }
}
