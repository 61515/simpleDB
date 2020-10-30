import os
import colorama
from colorama import Fore
import operator
import re


# 没有当前表 返回None
def read_table(tbName):
    tbPath = r"C:\Program Files\simpleDb"
    tbPath += "\\" + tbName + ".tb"
    list2_tb = []

    if os.path.exists(tbPath) is False:
        return None

    with open(tbPath, "r", encoding="utf-8") as f:
        data = f.readlines()
        for line in data:
            line = line.strip('\n')
            list_row = []
            for content in line.split('\t'):
                list_row.append(content)
            list2_tb.append(list_row)
        # 规划第一行的字段名
        for i in range(len(list2_tb[0])):
            list2_tb[0][i] = tbName + "." + list2_tb[0][i]
    return list2_tb


def write_table(tbName, list2_tb):
    tbPath = r"C:\Program Files\simpleDb"
    tbPath += "\\" + tbName + ".tb"

    for i in range(len(list2_tb[0])):
        if '.' in list2_tb[0][i]:
            list2_tb[0][i] = list2_tb[0][i].split('.')[1]

    with open(tbPath, "w", encoding="utf-8") as f:
        for list_row in list2_tb:
            for i in range(len(list_row)):
                content = list_row[i]
                if i > 0:
                    f.write('\t')
                f.write(content)
            f.write('\n')


# 读取多个空白字符
def read_mulspace(sql_sentence, index):
    list_space = [' ', '\t', '\n']
    while index < len(sql_sentence) and sql_sentence[index] in list_space:
        index += 1
    return index


# 读一个单词直到 空白
def read_a_word(sentence, index, another_space=None, except_space=None):
    list_space = ['\t', ' ', '\n']
    if another_space:
        for _space in another_space:
            list_space.append(_space)
    if except_space:
        for _space in except_space:
            list_space.remove(_space)

    while index < len(sentence):
        if sentence[index] in list_space:
            break
        else:
            index += 1

    return index


# 读取某个单词，可以指定是否大小写忽略
# 后一个参数 主要用于 order by 和 group by
# False index
# True  index
def read_word(sql_sentence, index, word, ignoreCase=True, has_another_spaces=False):
    for c in word:
        if index >= len(sql_sentence):
            return False, index

        if ignoreCase:
            if sql_sentence[index].lower() == c.lower():
                index += 1
            else:
                return False, index
        else:
            if sql_sentence[index] == c:
                index += 1
            else:
                return False, index

    if has_another_spaces:
        # 读入空格单元, 并读入by
        list_space = [' ', '\n', '\t']
        has_space = False
        while index < len(sql_sentence) and sql_sentence[index] in list_space:
            index += 1
            has_space = True

        if has_space:
            if not (index + 1 < len(sql_sentence) and sql_sentence[index].lower() == 'b'
                    and sql_sentence[index + 1].lower() == 'y'):
                return False, index
            else:
                index += 2
        else:
            return False, index
    return True, index


# 读取分句内容，直到有关键子出现，返回子句的索引
def read_partSentence(sql_sentence, index):
    keywords = ["Select", "From", "Where", "LIMIT", "Having", "Order", "Group"]

    while index < len(sql_sentence):
        # 找寻前五个所必须的关键字
        for i in range(7):
            # 判断字符串长度
            if index + len(keywords[i]) > len(sql_sentence):
                continue

            matched = False
            for j in range(len(keywords[i])):
                c_in_keyword = keywords[i][j]
                if c_in_keyword.lower() != sql_sentence[index + j].lower():
                    break
                if j == len(keywords[i]) - 1:
                    matched = True
            if matched:
                if i <= 4:
                    return index
                else:
                    # 处理中间有空位的位置
                    tmp_index = index + len(keywords[i])

                    # 读入空格单元
                    list_space = [' ', '\n', '\t']
                    has_space = False
                    while tmp_index < len(sql_sentence) and sql_sentence[tmp_index] in list_space:
                        tmp_index += 1
                        has_space = True
                    if has_space:
                        if tmp_index + 1 < len(sql_sentence) and sql_sentence[tmp_index].lower() == 'b' \
                                and sql_sentence[tmp_index + 1].lower() == 'y':
                            return index
        index += 1
    # 未发现关键字
    return index


# 运行状态
# 1 index 蓝色代表扫描成功的内容
# 2 {keywords:[], runstreams:[], runningstreams: []} 运行的数据流绿色,关键字黄色,正在运行的数据流 红色
# 3 list2_result
def parse_sql(sql_sentence):
    run_state = {"keywords": [], "runstreams": [], "runningstreams": []}
    dic_runstream = {}  # 每个子句的运行索引信息
    # 先扫描，扫描之后递归运行
    index = read_mulspace(sql_sentence, 0)

    # Select 关键字
    result_select = read_word(sql_sentence, index, "Select", ignoreCase=True)
    if result_select[0]:
        run_state["keywords"].append((index, result_select[1]))
        index = result_select[1]
    else:
        # 无Select 子句
        return 1, result_select[1]

    # 判断当前是否为space 或者 *
    list_space = [' ', '\t', '\n']
    if sql_sentence[index] not in list_space and sql_sentence[index] != '*':
        # 不符合Select 后跟子句的规则
        return 1, index

    index = read_mulspace(sql_sentence, index)

    tmp_index = read_partSentence(sql_sentence, index)
    sentence_select = sql_sentence[index: tmp_index].strip()
    dic_runstream["sentence_select"] = (index, tmp_index)
    index = tmp_index

    # From 关键字
    result_from = read_word(sql_sentence, index, "From", ignoreCase=True)
    if result_from[0]:
        run_state["keywords"].append((index, result_from[1]))
        index = result_from[1]
    else:
        # 无From 子句, 数据源 从From 开始
        return 1, result_from[1]

    if sql_sentence[index] not in list_space:
        return 1, index

    index = read_mulspace(sql_sentence, index)

    tmp_index = read_partSentence(sql_sentence, index)
    sentence_from = sql_sentence[index: tmp_index].strip()
    dic_runstream["sentence_from"] = (index, tmp_index)
    index = tmp_index

    # Where 关键字(可选)
    result_where = read_word(sql_sentence, index, "Where", ignoreCase=True)
    if result_where[0]:
        run_state["keywords"].append((index, result_where[1]))
        index = result_where[1]
        if sql_sentence[index] not in list_space:
            return 1, index

        index = read_mulspace(sql_sentence, index)

        tmp_index = read_partSentence(sql_sentence, index)
        sentence_where = sql_sentence[index: tmp_index].strip()
        dic_runstream["sentence_where"] = (index, tmp_index)
        index = tmp_index
    else:
        # 无Where 子句, 无Where 子句的信息
        sentence_where = None

    # Group by 关键字(可选)
    result_groupby = read_word(sql_sentence, index, "Group", ignoreCase=True, has_another_spaces=True)
    if result_groupby[0]:
        run_state["keywords"].append((index, result_groupby[1]))
        index = result_groupby[1]
        if sql_sentence[index] not in list_space:
            return 1, index

        index = read_mulspace(sql_sentence, index)

        tmp_index = read_partSentence(sql_sentence, index)
        sentence_groupby = sql_sentence[index: tmp_index].strip()
        dic_runstream["sentence_groupby"] = (index, tmp_index)
        index = tmp_index
    else:
        # 无Group by 子句
        sentence_groupby = None

    # having 关键字(可选)
    result_having = read_word(sql_sentence, index, "having", ignoreCase=True)
    if result_having[0]:
        run_state["keywords"].append((index, result_having[1]))
        index = result_having[1]
        if sql_sentence[index] not in list_space:
            return 1, index

        index = read_mulspace(sql_sentence, index)
        tmp_index = read_partSentence(sql_sentence, index)
        sentence_having = sql_sentence[index: tmp_index].strip()
        dic_runstream["sentence_having"] = (index, tmp_index)
        index = tmp_index
    else:
        sentence_having = None

    # Order by 关键字(可选)
    result_Order = read_word(sql_sentence, index, "Order", ignoreCase=True, has_another_spaces=True)
    if result_Order[0]:
        run_state["keywords"].append((index, result_Order[1]))
        index = result_Order[1]
        if sql_sentence[index] not in list_space:
            return 1, index

        index = read_mulspace(sql_sentence, index)
        tmp_index = read_partSentence(sql_sentence, index)
        sentence_orderby = sql_sentence[index: tmp_index].strip()
        dic_runstream["sentence_orderby"] = (index, tmp_index)
        index = tmp_index
    else:
        sentence_orderby = None

    # Limit 关键字(可选)
    result_Limit = read_word(sql_sentence, index, "Limit", ignoreCase=True)
    if result_Limit[0]:
        run_state["keywords"].append((index, result_Limit[1]))
        index = result_Limit[1]
        if sql_sentence[index] not in list_space:
            return 1, index

        index = read_mulspace(sql_sentence, index)
        tmp_index = read_partSentence(sql_sentence, index)
        sentence_limit = sql_sentence[index: tmp_index].strip()
        dic_runstream["sentence_limit"] = (index, tmp_index)
    else:
        sentence_limit = None

    # 若最后有分号，则去掉
    if sentence_limit:
        if sentence_limit[-1] == ';':
            sentence_limit = sentence_limit[:-1]
    elif sentence_orderby:
        if sentence_orderby[-1] == ';':
            sentence_orderby = sentence_orderby[:-1]
    elif sentence_having:
        if sentence_having[-1] == ';':
            sentence_having = sentence_having[:-1]
    elif sentence_groupby:
        if sentence_groupby[-1] == ';':
            sentence_groupby = sentence_groupby[:-1]
    elif sentence_where:
        if sentence_where[-1] == ';':
            sentence_where = sentence_where[:-1]
    elif sentence_from:
        if sentence_from[-1] == ';':
            sentence_from = sentence_from[:-1]
    elif sentence_select:
        if sentence_select[-1] == ';':
            sentence_select = sentence_select[:-1]

    # 扫描之后，按照顺序运行数据流(按照sql语句执行的顺序)
    run_state["runningstreams"].append(dic_runstream["sentence_from"])
    try:
        data_source = run_From(sentence_from)
    except Exception:
        return 2, run_state
    else:
        run_state["runstreams"].append(dic_runstream["sentence_from"])
        run_state["runningstreams"].remove(dic_runstream["sentence_from"])

    data = data_source
    # 运行 Where 选择结构
    if sentence_where:
        run_state["runningstreams"].append(dic_runstream["sentence_where"])
        try:
            data = run_where(data, sentence_where)
        except Exception:
            return 2, run_state
        else:
            run_state["runstreams"].append(dic_runstream["sentence_where"])
            run_state["runningstreams"].remove(dic_runstream["sentence_where"])
    # 运行 Group by 分组结构
    if sentence_groupby:
        run_state["runningstreams"].append(dic_runstream["sentence_groupby"])
        try:
            data = run_groupby(data, sentence_groupby)
            # 运行 Having 选择结构, 聚合函数用于筛选组
            if sentence_having:
                run_state["runningstreams"].append(dic_runstream["sentence_having"])
                try:
                    data = run_having(data, sentence_having)
                except Exception:
                    return 2, run_state
                else:
                    run_state["runstreams"].append(dic_runstream["sentence_having"])
                    run_state["runningstreams"].remove(dic_runstream["sentence_having"])
        except Exception:
            return 2, run_state
        else:
            run_state["runstreams"].append(dic_runstream["sentence_groupby"])
            run_state["runningstreams"].remove(dic_runstream["sentence_groupby"])
    # 运行 Select 选择字段
    run_state["runningstreams"].append(dic_runstream["sentence_select"])
    try:
        data = run_select(data, sentence_select)
    except Exception:
        return 2, run_state
    else:
        run_state["runstreams"].append(dic_runstream["sentence_select"])
        run_state["runningstreams"].remove(dic_runstream["sentence_select"])
    # 运行 Order by 排序结构
    if sentence_orderby:
        run_state["runningstreams"].append(dic_runstream["sentence_orderby"])
        try:
            data = run_orderby(data, sentence_orderby)
        except Exception:
            return 2, run_state
        else:
            run_state["runstreams"].append(dic_runstream["sentence_orderby"])
            run_state["runningstreams"].remove(dic_runstream["sentence_orderby"])
    # 运行 Limit 限制最后输出的条数
    if sentence_limit:
        run_state["runningstreams"].append(dic_runstream["sentence_limit"])
        try:
            data = run_limit(data, sentence_limit)
        except Exception:
            return 2, run_state
        else:
            run_state["runstreams"].append(dic_runstream["sentence_limit"])
            run_state["runningstreams"].remove(dic_runstream["sentence_limit"])

    # 返回状态3和最后运行流的结果
    return 3, data


def run_limit(data, sentence_limit):
    sentence_limit = sentence_limit.strip()
    new_data = data
    if sentence_limit.isdigit():
        num = int(sentence_limit)
        if num < len(data) - 1:
            new_data = [data[0]]
            for i in range(num):
                new_data.append(data[i + 1])
    return new_data


def run_orderby(data, sentence_orderby):
    # 以 ',' 分割排序 条件
    if ',' in sentence_orderby:
        list_sentence_part_orderby = sentence_orderby.split(',')
        for sentence_part_orderby in reversed(list_sentence_part_orderby):
            data = run_orderby(data, sentence_part_orderby)
        return data
    else:
        # SC.Grade
        index = read_mulspace(sentence_orderby, 0)
        tmp_index = read_a_word(sentence_orderby, index)
        field = sentence_orderby[index: tmp_index]

        if "." not in field:
            for title in data[0]:
                if field == title.split('.')[1]:
                    field = title
                    break

        index = -1
        for i in range(len(data[0])):
            title = data[0][i]
            if title == field:
                index = i
                break

        # 选择排序
        for i in range(1, len(data)):
            index2 = i
            for j in range(i + 1, len(data)):
                if data[j][index] < data[index2][index]:
                    index2 = j
            if index2 != i:
                list_tmp = data[i]
                data[i] = data[index2]
                data[index2] = list_tmp

        # 判断是否是 递减的
        index = tmp_index
        index = read_mulspace(sentence_orderby, index)
        tmp_index = read_a_word(sentence_orderby, index)
        word = sentence_orderby[index:tmp_index]
        if word and len(word) > 0:
            if word.lower() == 'asc'.lower():
                return data
            elif word.lower() == 'desc'.lower():
                new_data = [data[0]]
                for each_line in reversed(data[1:]):
                    new_data.append(each_line)
                return new_data
        return data


def run_select(data, sentence_select):
    # Student.Sname, Course.Cname, SC.Grade
    sentence_select = sentence_select.strip()
    list_fields = sentence_select.split(',')
    for i in range(len(list_fields)):
        field = list_fields[i].strip()

        if "." not in field:
            for j in range(len(data[0])):
                if field == data[0][j].split('.')[1]:
                    field = data[0][j]
                    break
        list_fields[i] = field
    # 字段

    list_indexs = []
    for field_select in list_fields:
        for i in range(len(data[0])):
            title = data[0][i]
            if title == field_select:
                list_indexs.append(i)
                break
        else:
            list_indexs.append(field_select)

    new_data = [[]]
    # 处理头信息
    exist_agg_func = False
    for content_index in list_indexs:
        if str(content_index).lower().startswith("count"):
            new_data[0].append(content_index)
            exist_agg_func = True
        elif str(content_index) == '*':
            for content in data[0]:
                new_data[0].append(content)
        else:
            new_data[0].append(data[0][content_index])

    if exist_agg_func:
        if isinstance(data[1][0], list):
            for each_group in data[1:]:
                new_line = []
                for content_index in list_indexs:
                    # 判断是否有聚合函数
                    if str(content_index).lower().startswith("count"):
                        # 找到要 Count 的字段
                        indexl = content_index.index('(')
                        indexr = content_index.index(')')
                        field = content_index[indexl + 1: indexr].strip()
                        new_group = [data[0]]
                        for line in each_group:
                            new_group.append(line)
                        new_line.append(agg_func(new_group, "Count", field))
                    elif str(content_index) == '*':
                        for content in each_group[0]:
                            new_line.append(content)
                    else:
                        new_line.append(each_group[0][content_index])
                new_data.append(new_line)
        else:
            for each_line in data[1:]:
                new_line = []
                ok = False
                for content_index in list_indexs:
                    # 判断是否有聚合函数
                    if str(content_index).lower().startswith("count"):
                        # 找到要 Count 的字段
                        indexl = content_index.index('(')
                        indexr = content_index.index(')')
                        field = content_index[indexl + 1: indexr].strip()
                        new_line.append(agg_func(data, "Count", field))
                        ok = True
                    elif str(content_index) == '*':
                        for content in each_line:
                            new_line.append(content)
                    else:
                        new_line.append(each_line[content_index])
                new_data.append(new_line)
                if ok:
                    break
    else:
        for group_line in data[1:]:
            if isinstance(group_line[0], list):
                group_line = group_line[0]

            new_line = []
            for content_index in list_indexs:
                # 判断是否有聚合函数
                if str(content_index) == '*':
                    for content in group_line:
                        new_line.append(content)
                else:
                    new_line.append(group_line[content_index])
            new_data.append(new_line)
    return new_data


def check_relops(field1, ops, field2):
    if ops == '<':
        return field1 < field2
    elif ops == '>':
        return field1 > field2
    elif ops == '=':
        return field1 == field2
    elif ops == "!=":
        return field1 != field2
    elif ops == ">=":
        return field1 >= field2
    elif ops == "<=":
        return field1 <= field2
    return True


def agg_func(group, func_name, field='*'):
    if func_name.lower() == "Count".lower():
        if field == '*':
            return len(group) - 1

        index = -1
        if "." not in field:
            for i in range(len(group[0])):
                if field == group[0][i].split('.')[1]:
                    index = i
        else:
            for i in range(len(group[0])):
                if field == group[0][i]:
                    index = i
        _len = 0
        for each_line in group[1:]:
            if each_line[index] != "":
                _len += 1
        return _len
    return None


def run_having(data, sentence_having):
    # Count(*) >= 2 某个聚合函数
    list_ops = ["!=", ">=", "<=", '<', '>', '=']
    # 以列表中的符号左右添加 空格
    for op in list_ops:
        if op in sentence_having:
            index = sentence_having.index(op)
            _len = len(op)
            sentence_having = sentence_having[:index] + " " + sentence_having[index:index + _len] + " " + \
                             sentence_having[index + _len:]
            break

    index = read_mulspace(sentence_having, 0)
    tmp_index = read_a_word(sentence_having, index, another_space=['('])
    func_name = sentence_having[index: tmp_index].strip()
    index = tmp_index + 1

    tmp_index = read_a_word(sentence_having, index, another_space=[')'])
    params = sentence_having[index: tmp_index]
    list_params = params.strip().split(',')
    for i in range(len(list_params)):
        list_params[i] = list_params[i].strip()
    index = tmp_index + 1

    index = read_mulspace(sentence_having, index)

    tmp_index = read_a_word(sentence_having, index)
    ops = sentence_having[index: tmp_index]
    index = tmp_index

    index = read_mulspace(sentence_having, index)

    tmp_index = read_a_word(sentence_having, index)
    field2 = sentence_having[index: tmp_index]

    new_data = [data[0]]

    # 暂时处理Count函数
    if func_name.lower() == "Count".lower():
        if field2.isdigit():
            field2 = int(field2)
        # 对每一个组 运行聚合函数筛选
        for each_group in data[1:]:
            new_group = [data[0]]
            for each_line in each_group:
                new_group.append(each_line)
            num = agg_func(new_group, func_name, list_params[0])
            if check_relops(num, ops, field2):
                new_data.append(each_group)
    return new_data


def run_groupby(data, sentence_groupby):
    # SC.Grade
    index = read_mulspace(sentence_groupby, 0)
    tmp_index = read_a_word(sentence_groupby, index)
    # 要分类的字段名称
    field = sentence_groupby[index: tmp_index]
    # 实质是将数据 划分为三维列表的子表类型

    index = -1
    if "." not in field:
        for i in range(len(data[0])):
            if field == data[0][i].split('.')[1]:
                index = i
                break
    else:
        for i in range(len(data[0])):
            if field == data[0][i]:
                index = i
                break

    if index == -1:
        return data
    newdata = [data[0]]  # 标题
    for each_line in data[1:]:
        content = each_line[index]
        ok = False

        for i in range(1, len(newdata)):
            each_group = newdata[i]
            if content == each_group[0][index]:
                newdata[i].append(each_line)
                ok = True
                break

        if ok is False:
            group = [each_line]
            newdata.append(group)

    return newdata


def run_where(data, sentence_where):
    # SC.Cno = Course.Cno
    # 暂且只支持 > < = != >= <=
    list_ops = ["!=", ">=", "<=", '<', '>', '=']
    # 以列表中的符号左右添加 空格
    for op in list_ops:
        if op in sentence_where:
            index = sentence_where.index(op)
            _len = len(op)
            sentence_where = sentence_where[:index] + " " + sentence_where[index:index+_len] + " " + \
                sentence_where[index+_len:]
            break

    index = read_mulspace(sentence_where, 0)

    tmp_index = read_a_word(sentence_where, index)
    field1 = sentence_where[index: tmp_index]
    index = tmp_index

    index = read_mulspace(sentence_where, index)

    tmp_index = read_a_word(sentence_where, index)
    ops = sentence_where[index: tmp_index]
    index = tmp_index

    if ops in list_ops:
        index = read_mulspace(sentence_where, index)

        tmp_index = read_a_word(sentence_where, index)
        field2 = sentence_where[index: tmp_index]
        # 预处理 int,str 类型

        type1 = "str"
        type2 = "str"
        list_type = ["int", "str", "float"]
        for _type in list_type:
            if field1.startswith(_type):
                type1 = _type
                index_l = field1.index('(')
                index_r = field1.index(')')
                field1 = field1[index_l + 1: index_r]
                if field1 is None or len(field1) <= 0:
                    raise Exception

            if field2.startswith(_type):
                type2 = _type
                index_l = field2.index('(')
                index_r = field2.index(')')
                field2 = field2[index_l + 1: index_r]
                if field2 is None or len(field2) <= 0:
                    raise Exception
        # 确定字段的正确性

        index1 = -1
        index2 = -1
        if "." not in field1:
            for i in range(len(data[0])):
                field = data[0][i]
                if field1 == field.split('.')[1]:
                    index1 = i
        else:
            for i in range(len(data[0])):
                field = data[0][i]
                if field == field1:
                    index1 = i
        if "." not in field2:
            for i in range(len(data[0])):
                field = data[0][i]
                if field2 == field.split('.')[1]:
                    index2 = i
        else:
            for i in range(len(data[0])):
                field = data[0][i]
                if field == field2:
                    index2 = i

        # 进行字段的判断
        new_data = [data[0]]

        for each_line in data[1:]:
            if index1 != -1:
                content1 = each_line[index1]
            else:
                content1 = field1

            if index2 != -1:
                content2 = each_line[index2]
            else:
                content2 = field2

            # 若字段有引号 则去掉
            if content1[0] == '\'' and content1[-1] == '\'' or \
                    content1[0] == '\"' and content1[-1] == '\"':
                content1 = content1[1:-1]

            if content2[0] == '\'' and content2[-1] == '\'' or \
                    content2[0] == '\"' and content2[-1] == '\"':
                content2 = content2[1:-1]

            if type1 == "int":
                content1 = int(content1)
            elif type1 == "float":
                content1 = float(content1)
            elif type1 == "str":
                content1 = str(content1)

            if type2 == "int":
                content2 = int(content2)
            elif type2 == "float":
                content2 = float(content2)
            elif type2 == "str":
                content2 = str(content2)

            if check_relops(content1, ops, content2):
                new_data.append(each_line)
        return new_data

    else:
        raise Exception


# 第一个参数 True,False ,第二个参数 结果源
def run_From(sentence_from):
    # 例:Student Join SC on SC.Sno = Student.Sno,Course
    # 以,分割子句
    list_Clause = sentence_from.split(',')
    table_union = None
    for clause in list_Clause:
        if "Join".lower() in clause.lower() and \
                "On".lower() in clause.lower():
            # 两表联合,形成数据Where条件筛选后 再连接表
            index = read_mulspace(clause, 0)

            tmp_index = read_a_word(clause, index)
            table1_name = clause[index: tmp_index]
            index = tmp_index

            index = read_mulspace(clause, index)

            result1 = read_word(clause, index, "Join", ignoreCase=True)
            if result1[0]:
                index = result1[1]

                index = read_mulspace(clause, index)

                tmp_index = read_a_word(clause, index)
                table2_name = clause[index: tmp_index]
                index = tmp_index

                index = read_mulspace(clause, index)

                result2 = read_word(clause, index, "On", ignoreCase=True)
                if result2[0]:
                    index = result2[1]

                    index = read_mulspace(clause, index)
                    condition = clause[index:]

                    table1 = read_table(table1_name)
                    table2 = read_table(table2_name)

                    if table1 and table2:
                        table_link = union_table(table1, table2)
                    else:
                        continue
                    # 条件筛选
                    table_link = run_where(table_link, condition)
                    # 总表连接
                    if table_union:
                        table_union = union_table(table_union, table_link)
                    else:
                        table_union = table_link

        else:

            index = read_mulspace(clause, 0)
            tmp_index = read_a_word(clause, index)
            table_name = clause[index:tmp_index]
            # 直接转化为 二维列表 table 结构
            table = read_table(table_name)
            if table:
                if table_union:
                    table_union = union_table(table_union, table)
                else:
                    table_union = table

    return table_union


def union_table(table1, table2):
    # union_title
    table_union = [table1[0] + table2[0]]
    # union_content
    for list_line_1 in table1[1:]:
        for list_line_2 in table2[1:]:
            table_union.append(list_line_1 + list_line_2)
    return table_union


def read_quo(sentence, index):
    quo = sentence[index]
    index += 1
    while index < len(sentence) and sentence[index] != quo:
        index += 1
    return index


def create_table(sentence):
    # table "table_name";
    result = read_word(sentence, 0, "Table", ignoreCase=True)
    if result[0] is False:
        return False
    index = result[1]

    index = read_mulspace(sentence, index)

    if sentence[index] == "\"" or sentence[index] == "\'":
        tmp_index = read_quo(sentence, index)
        table_name = sentence[index + 1: tmp_index].strip()
    else:
        tmp_index = read_a_word(sentence, index)
        table_name = sentence[index:tmp_index]
        if table_name[-1] == ';':
            table_name = table_name[:-1]
    # 以 table_name 建立文件
    tbPath = r"C:\Program Files\simpleDb"
    if os.path.exists(tbPath) is False:
        os.makedirs(tbPath)

    print("请输入内容,以空行结束")
    str_content = read_multi_lines()
    # 切割成二维列表，保存入文件
    list2_content = []
    col_num = -1
    for each_line in str_content.split('\n'):
        if each_line is False or len(each_line) == 0:
            continue
        list_content = []
        for content in re.split(r"[\t| ]", each_line):
            list_content.append(content)
        print(list_content)
        if col_num == -1:
            col_num = len(list_content)
        elif col_num != len(list_content):
            return False

        list2_content.append(list_content)
    write_table(table_name, list2_content)
    return True


def drop_table(sentence):
    # table "table_name"
    result = read_word(sentence, 0, "Table", ignoreCase=True)
    if result[0] is False:
        return False
    index = result[1]

    index = read_mulspace(sentence, index)

    if sentence[index] == "\"" or sentence[index] == "\'":
        tmp_index = read_quo(sentence, index)
        table_name = sentence[index + 1: tmp_index].strip()
    else:
        tmp_index = read_a_word(sentence, index)
        table_name = sentence[index:tmp_index]
        if table_name[-1] == ';':
            table_name = table_name[:-1]
    tbPath = r"C:\Program Files\simpleDb"
    # 如果存在当前文件，则删除
    if os.path.exists(os.path.join(tbPath, table_name + ".tb")):
        os.remove(os.path.join(tbPath, table_name + ".tb"))
        return True
    else:
        return False


def insert_data(sentence):
    # INTO "abc"
    # (Cno, Cname, Cpno, Ccredit)
    # VALUES
    # (8, "C语言", 5, 4);
    index = read_mulspace(sentence, 0)
    result1 = read_word(sentence, index, "INTO", ignoreCase=True)
    if result1[0] is False:
        return False
    index = result1[1]
    index = read_mulspace(sentence, index)

    if sentence[index] == "\"" or sentence[index] == "\'":
        tmp_index = read_quo(sentence, index)
        table_name = sentence[index + 1: tmp_index].strip()
        index = tmp_index + 1
    else:
        tmp_index = read_a_word(sentence, index, another_space=['('])
        table_name = sentence[index:tmp_index]
        index = tmp_index

    index = read_mulspace(sentence, index)
    if sentence[index] != '(':
        return False
    tmp_index = read_a_word(sentence, index, another_space=[')'], except_space=[' ', '\t', '\n'])
    list_fields = sentence[index + 1: tmp_index].strip().split(',')
    index = tmp_index + 1

    # 读入Values
    index = read_mulspace(sentence, index)
    result2 = read_word(sentence, index, "Values", ignoreCase=True)
    if result2[0] is False:
        return False
    index = result2[1]

    index = read_mulspace(sentence, index)
    if sentence[index] != '(':
        return False

    tmp_index = read_a_word(sentence, index, another_space=[')'], except_space=[' ', '\t', '\n'])
    list_contents = sentence[index + 1: tmp_index].strip().split(',')

    # 校验字段和内容
    data = read_table(table_name)
    title = data[0]
    for i in range(len(list_fields)):
        list_fields[i] = list_fields[i].strip()
        if list_fields[i][0] == '\"' and list_fields[i][-1] == "\"" or \
                list_fields[i][0] == '\'' and list_fields[i][-1] == "\'":
            list_fields[i] = list_fields[i][1:-1]
        if "." not in list_fields[i]:
            for field in title:
                if list_fields[i] == field.split('.')[1]:
                    list_fields[i] = field
                    break

    for i in range(len(list_contents)):
        list_contents[i] = list_contents[i].strip()
        content = list_contents[i]
        if content[0] == '\"' and content[-1] == "\"" or \
                content[0] == '\'' and content[-1] == "\'":
            list_contents[i] = content[1:-1]

    # 读表并操纵列表结构
    list_index = []
    for field in list_fields:
        for i in range(len(title)):
            if field == title[i]:
                list_index.append(i)
                break

    if len(list_index) != len(title):
        return False

    line = []
    for index in list_index:
        line.append(list_contents[index])
    data.append(line)

    write_table(table_name, data)
    return True


def delete_data(sentence):
    # from "abc" Where Cno = 8
    index = read_mulspace(sentence, 0)
    result1 = read_word(sentence, index, "From", ignoreCase=True)
    if result1[0] is False:
        return False
    index = result1[1]

    index = read_mulspace(sentence, index)
    if sentence[index] == "\"" or sentence[index] == "\'":
        tmp_index = read_quo(sentence, index)
        table_name = sentence[index + 1: tmp_index].strip()
        index = tmp_index + 1
    else:
        tmp_index = read_a_word(sentence, index)
        table_name = sentence[index:tmp_index]
        index = tmp_index

    index = read_mulspace(sentence, index)
    # 读入 Where
    result2 = read_word(sentence, index, "Where", ignoreCase=True)
    if result2[0] is False:
        return False

    index = result2[1]
    index = read_mulspace(sentence, index)

    Condition = sentence[index:].strip()
    if Condition[-1] == ';':
        Condition = Condition[:-1]

    data = read_table(table_name)
    del_data = run_where(data, Condition)

    for each_line in data[1:]:
        for del_line in del_data[1:]:
            if operator.eq(each_line, del_line):
                data.remove(del_line)
                break

    write_table(table_name, data)
    return True


def update_data(sentence):
    # "abc" Set Ccredit = 5 Where Cno=8
    index = read_mulspace(sentence, 0)
    if sentence[index] == "\"" or sentence[index] == "\'":
        tmp_index = read_quo(sentence, index)
        table_name = sentence[index + 1: tmp_index].strip()
        index = tmp_index + 1
    else:
        tmp_index = read_a_word(sentence, index)
        table_name = sentence[index:tmp_index]
        index = tmp_index

    index = read_mulspace(sentence, index)
    # 读入Set
    result_set = read_word(sentence, index, "Set", ignoreCase=True)
    if result_set[0] is False:
        return False

    index = result_set[1]
    index = read_mulspace(sentence, index)
    # 读到 Where 单词
    tmp_index = index
    while tmp_index + 4 < len(sentence):
        word = sentence[tmp_index:tmp_index + 5]
        if word.lower() == "Where".lower():
            break
        else:
            tmp_index += 1
    sentence_asign = sentence[index:tmp_index].strip()
    index = tmp_index
    # 读入 Where
    result_where = read_word(sentence, index, "Where", ignoreCase=True)
    if result_where[0] is False:
        return False
    index = result_where[1]

    index = read_mulspace(sentence, index)
    condition = sentence[index:].strip()
    if condition[-1] == ';':
        condition = condition[:-1]

    data = read_table(table_name)
    data_update = run_where(data, condition)

    if len(sentence_asign.strip('=')) < 2:
        return False

    field = sentence_asign.split('=')[0].strip()
    content = sentence_asign.split('=')[1].strip()
    # 矫正字段
    title = data[0]
    index = -1
    for i in range(len(title)):
        if field == title[i].split('.')[1]:
            index = i
            break
        elif field == title[i]:
            index = i
            break

    if index == -1:
        return False

    for i in range(1, len(data)):
        for line_update in data_update[1:]:
            if operator.eq(data[i], line_update):
                # 要更新的数据行
                data[i][index] = content
    write_table(table_name, data)
    return True


def show_tables(sentence):
    index = read_mulspace(sentence, 0)
    tmp_index = read_a_word(sentence, index)
    word = sentence[index: tmp_index]
    if word[-1] == ";":
        word = word[:-1]

    if word.lower() == "tables".lower():
        tbPath = r"C:\Program Files\simpleDb"
        if os.path.exists(tbPath) is False:
            print("None")
        else:
            exist = False
            for root, dirs, files in os.walk(tbPath):
                # root 表示当前正在访问的文件夹路径
                # dirs 表示该文件夹下的子目录名list
                # files 表示该文件夹下的文件list
                for tb in files:
                    exist = True
                    print(tb.split('.')[0], end='\t')
                print()
            if exist is False:
                print("None")


# 运行模式1,可以进行建表，删表，更删改等语句操作
def run_pattern1(sentence):
    index = read_mulspace(sentence, 0)
    tmp_index = read_a_word(sentence, index)
    instructions = sentence[index: tmp_index]
    index = tmp_index

    index = read_mulspace(sentence, index)

    if instructions.lower() == "Create".lower():
        try:
            result = create_table(sentence[index:])
        except Exception:
            print("创建表失败")
        else:
            if result:
                print("创建表成功")
            else:
                print("创建表失败")
    elif instructions.lower() == "Drop".lower():
        try:
            result = drop_table(sentence[index:])
        except Exception:
            print("删除表失败")
        else:
            if result:
                print("删除表成功")
            else:
                print("删除表失败")
    elif instructions.lower() == "Insert".lower():
        try:
            result = insert_data(sentence[index:])
        except Exception:
            print("插入数据失败")
        else:
            if result:
                print("插入数据成功")
            else:
                print("插入数据失败")
    elif instructions.lower() == "Delete".lower():
        try:
            result = delete_data(sentence[index:])
        except Exception:
            print("删除数据失败")
        else:
            if result:
                print("删除数据成功")
            else:
                print("删除数据失败")
    elif instructions.lower() == "Update".lower():
        try:
            result = update_data(sentence[index:])
        except Exception:
            print("更新数据失败")
        else:
            if result:
                print("更新数据成功")
            else:
                print("更新数据失败")
    elif instructions.lower() == "Show".lower():
        try:
            show_tables(sentence[index:])
        except Exception:
            print("Show 命令异常")
    else:
        print("未识别的指令")


# 运行模式2,可以进行sql语句的解析，执行结果输出
def run_pattern2(sql_sentence):
    colorama.init()
    result = parse_sql(sql_sentence)
    if result[0] == 1:
        print(Fore.BLUE + sql_sentence[:result[1]], end='')
        print(Fore.WHITE + sql_sentence[result[1]:])
    elif result[0] == 2:
        # {keywords: [], runstreams: [], runningstream: []}
        # 运行的数据流绿色, 关键字黄色, 正在运行的数据流
        # 红色
        keywords = result[1]["keywords"]
        runstreams = result[1]["runstreams"]
        runningstreams = result[1]["runningstreams"]
        for i in range(len(sql_sentence)):
            yellow = False
            for keyword in keywords:
                if keyword[0] <= i <= keyword[1]:
                    yellow = True
                    break
            green = False
            for runstream in runstreams:
                if runstream[0] <= i <= runstream[1]:
                    green = True
                    break
            red = False
            for runningstream in runningstreams:
                if runningstream[0] <= i <= runningstream[1]:
                    red = True
                    break
            if yellow:
                print(Fore.YELLOW + sql_sentence[i], end='')
            elif green:
                print(Fore.GREEN + sql_sentence[i], end='')
            elif red:
                print(Fore.RED + sql_sentence[i], end='')
            else:
                print(Fore.WHITE + sql_sentence[i], end='')
        print()
    elif result[0] == 3:

        data = result[1]
        list_maxchar = []
        for i in range(len(data[0])):
            list_maxchar.append(10)

        for i in range(len(data)):
            each_line = data[i]
            for j in range(len(each_line)):
                content = str(data[i][j])
                _len = 0
                for ch in content:
                    if '\u4e00' <= ch <= '\u9fff':
                        _len += 2
                    else:
                        _len += 1
                if _len > list_maxchar[j]:
                    list_maxchar[j] = (int((_len - 1) / 10) + 1) * 10

        # 打印最后结果

        for i in range(len(data)):
            each_line = data[i]
            for j in range(len(each_line)):
                content = str(data[i][j])
                _len = 0
                for ch in content:
                    if '\u4e00' <= ch <= '\u9fff':
                        _len += 1
                content_print = content.rjust(list_maxchar[j] - _len, ' ')
                print(content_print, end='|')
            print()


# 运行模式3，可以进行python语句的直接执行
def run_pattern3(sentence):
    try:
        exec(sentence)
    except Exception:
        print("执行Python 语句失败")


def read_multi_lines():
    content = ""
    while True:
        line = input()
        if not line:
            break
        content += line + "\n"
    return content


if __name__ == '__main__':
    colorama.init()
    run_mode = 2
    First = True
    while True:
        if run_mode == 1:
            if First:
                print(Fore.WHITE + "请输入建表，删表语句，#id转换模式,空行结束")
                First = False
        elif run_mode == 2:
            if First:
                print(Fore.WHITE + "请输入sql语句，#id转换模式,空行结束")
                First = False
        elif run_mode == 3:
            if First:
                print(Fore.WHITE + "请输入python 语句，直接调用本类库函数，#id转换模式,空行结束")
                First = False

        sentence = read_multi_lines()

        index = read_mulspace(sentence, 0)
        if index < len(sentence) and sentence[index] == '#':
            index += 1

            index = read_mulspace(sentence, index)
            if index < len(sentence):
                _id = sentence[index]
                if _id == '1':
                    run_mode = 1
                    print(Fore.WHITE + "切换成功模式1")
                    First = True
                    continue
                elif _id == '2':
                    run_mode = 2
                    First = True
                    print(Fore.WHITE + "切换成功模式2")
                    continue
                elif _id == '3':
                    run_mode = 3
                    First = True
                    print(Fore.WHITE + "切换成功模式3")
                    continue

        # 判断是否转换输入模式

        if run_mode == 1:
            run_pattern1(sentence)
        elif run_mode == 2:
            run_pattern2(sentence)
        elif run_mode == 3:
            run_pattern3(sentence)
        print(Fore.WHITE + "", end='')
