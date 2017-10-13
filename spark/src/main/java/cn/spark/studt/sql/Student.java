package cn.spark.studt.sql;

import scala.runtime.StringFormat;

import java.io.Serializable;

/**
 * Created by Zouyy on 2017/9/14.
 */
public class Student implements Serializable{


    private int id;
    private String name;
    private int age;

    public Student(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return String.format("student:id==%d\tstudent:name==%s\tstudent:age==%o",getId(),getName(),getAge());
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
