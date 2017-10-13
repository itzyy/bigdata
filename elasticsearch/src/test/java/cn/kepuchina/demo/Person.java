package cn.kepuchina.demo;

import junit.framework.TestCase;

import java.io.Serializable;

/**
 * Created by Zouyy on 2017/8/17.
 */
public class Person extends TestCase {


    private String first_name;
    private String last_name;
    private int age;
    private String[] interets;

    public String getFirst_name() {
        return first_name;
    }

    public void setFirst_name(String first_name) {
        this.first_name = first_name;
    }

    public String getLast_name() {
        return last_name;
    }

    public void setLast_name(String last_name) {
        this.last_name = last_name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String[] getInterets() {
        return interets;
    }

    public void setInterets(String[] interets) {
        this.interets = interets;
    }

    public String getAbout() {
        return about;
    }

    public void setAbout(String about) {
        this.about = about;
    }

    private String about;

    public Person(String first_name, String last_name, int age, String[] interets, String about) {
        this.first_name = first_name;
        this.last_name = last_name;
        this.age = age;
        this.interets = interets;
        this.about = about;
    }
}
