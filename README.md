模式1:建表，删表语句，更删改语句示例

    a.Create table "table_name";
    输入数据(以制表符或空格分割数据,以回车符作为一行的结束):
    Cno	Cname	Cpno	Ccredit
    1	数据库	5	4
    2	数学		2
    3	信息系统	1	4
    4	操作系统	6	3
    5	数据结构	7	4
    6	数据处理		2
    7	PASCAL语言	6	4
    
    b.删除表
    Drop table "table_name"
    
    c.在表中插入数据
    INSERT INTO "Course" 
    (Cno, Cname, Cpno, Ccredit)
    VALUES
    (8, "C语言", 5, 4);
    
    d.在表中删除数据
    Delete from "Course" Where Cno = 8
    
    e.在表中更新数据
    Update "Course" Set Ccredit = 5 Where Cno=8
    
    f.显式所有的表
    Show tables;
模拟3:sql语句示例

    Select
        Student.Sname,Course.Cname,SC.Grade
    From
        Student Join SC on SC.Sno = Student.Sno,Course
    Where
        SC.Cno = Course.Cno
    Group by
        SC.Cno
    Having
        Count(*) >= 2
    Order by
        SC.Grade
    LIMIT
        2;

模式3示例:

    本程序适用于数据量不大且数据逻辑不复杂时，可以比较方便的直接对数据进行操作
    例如，计算本班成绩绩点

说明:

    数据表在程序中"二维列表"数据结构表示
    数据的修改建议直接修改.tb文件
    默认第一行为表头，且无字段约束，所以在输入sql语句的时候要加上类型,例如int(Student.Sname)
    sql不支持 嵌套语句查询
    为了轻量，只建立数据表
    解释sql语句的过程有时采用递归,每个数据流用函数表示
    聚合函数只支持 Count 函数，条件语句只支持简单条件，不支持and or ()
    再次开发 可以增加 Where复杂条件，多种聚合函数，某种嵌套查询的方式
    针对编译时的一些错误，大多采用continue的方式略过，要想变成强类型的方式，要在内部引起异常
    对于底层的存储引擎仅仅用文件来表示
    本人学艺不精，程序内还存在某些未知错误