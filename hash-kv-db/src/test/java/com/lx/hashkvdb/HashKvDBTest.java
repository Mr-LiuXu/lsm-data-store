package com.lx.hashkvdb;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class HashKvDBTest {
    @Test
    public void put() throws IOException {
        HashKvDB kvDB = new HashKvDB("D:\\Users\\liuxu\\hashkvdb");
        kvDB.start();
        for (int i=0;i<30;i++){
            kvDB.put(String.valueOf(i),String.valueOf(i));
        }
        kvDB.remove("0");
        Assert.assertEquals(false,kvDB.get("0").isPresent());
        for (int i=1;i<20;i++){
            System.out.println("i="+i+",db.get(String.valueOf(i)).get()="+kvDB.get(String.valueOf(i)).get());
        }

    }
}
