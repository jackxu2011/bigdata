package me.xuling.geek.bigdata.week02;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

/**
 * @author jack
 * @since 2022/3/18
 **/
public class Student {

    private String name;

    private String studentId;

    private Integer classNo;

    private Integer understandingScore;

    private Integer programmingScore;

    public byte[] getRowKey() {
        return StringUtils.reverse(studentId).getBytes(StandardCharsets.UTF_8);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Student withName(String name) {
        this.name = name;
        return this;
    }

    public String getStudentId() {
        return studentId;
    }

    public void setStudentId(String studentId) {
        this.studentId = studentId;
    }

    public Student withStudentId(String studentId) {
        this.studentId = studentId;
        return this;
    }

    public Integer getClassNo() {
        return classNo;
    }

    public void setClassNo(Integer classNo) {
        this.classNo = classNo;
    }

    public Student withClassNo(Integer classNo) {
        this.classNo = classNo;
        return this;
    }

    public Integer getUnderstandingScore() {
        return understandingScore;
    }

    public void setUnderstandingScore(Integer understandingScore) {
        this.understandingScore = understandingScore;
    }

    public Student withUnderstandingScore(Integer understandingScore) {
        this.understandingScore = understandingScore;
        return this;
    }

    public Integer getProgrammingScore() {
        return programmingScore;
    }

    public void setProgrammingScore(Integer programmingScore) {
        this.programmingScore = programmingScore;
    }

    public Student withProgrammingScore(Integer programmingScore) {
        this.programmingScore = programmingScore;
        return this;
    }
}
