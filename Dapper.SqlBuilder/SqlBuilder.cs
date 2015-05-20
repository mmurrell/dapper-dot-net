using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper
{
    public class SqlBuilder
    {
        Dictionary<string, Clauses> data = new Dictionary<string, Clauses>();
        int seq;

        class Clause
        {
            public string Sql { get; set; }
            public object Parameters { get; set; }
            public bool IsInclusive { get; set; }
            public string UniqueKey { get; set; }
        }

        class Clauses : List<Clause>
        {
            string joiner;
            string prefix;
            string postfix;

            public Clauses(string joiner, string prefix = "", string postfix = "")
            {
                this.joiner = joiner;
                this.prefix = prefix;
                this.postfix = postfix;
            }

            public string ResolveClauses(DynamicParameters p)
            {
                foreach (var item in this)
                {
                    p.AddDynamicParams(item.Parameters);
                }
                return this.Any(a => a.IsInclusive)
                    ? prefix +
                      string.Join(joiner,
                          this.Where(a => !a.IsInclusive)
                              .Select(c => c.Sql)
                              .Union(new[]
                              {
                                  " ( " +
                                  string.Join(" OR ", this.Where(a => a.IsInclusive).Select(c => c.Sql).ToArray()) +
                                  " ) "
                              })) + postfix
                    : prefix + string.Join(joiner, this.Select(c => c.Sql)) + postfix;
            }
        }

        public class Template
        {
            readonly string sql;
            readonly SqlBuilder builder;
            readonly object initParams;
            int dataSeq = -1; // Unresolved

            public Template(SqlBuilder builder, string sql, dynamic parameters)
            {
                this.initParams = parameters;
                this.sql = sql;
                this.builder = builder;
            }

            static System.Text.RegularExpressions.Regex regex =
                new System.Text.RegularExpressions.Regex(@"\/\*\*.+\*\*\/", System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.Multiline);

            void ResolveSql()
            {
                if (dataSeq != builder.seq)
                {
                    DynamicParameters p = new DynamicParameters(initParams);

                    rawSql = sql;

                    foreach (var pair in builder.data)
                    {
                        rawSql = rawSql.Replace("/**" + pair.Key + "**/", pair.Value.ResolveClauses(p));
                    }
                    parameters = p;

                    // replace all that is left with empty
                    rawSql = regex.Replace(rawSql, "");

                    dataSeq = builder.seq;
                }
            }

            string rawSql;
            object parameters;

            public string RawSql { get { ResolveSql(); return rawSql; } }
            public object Parameters { get { ResolveSql(); return parameters; } }
        }


        public SqlBuilder()
        {
        }

        public Template AddTemplate(string sql, dynamic parameters = null)
        {
            return new Template(this, sql, parameters);
        }

        void AddClause(string name, string sql, object parameters, string joiner, string prefix = "", string postfix = "", bool IsInclusive = false, string uniqueKey = null)
        {
            Clauses clauses;
            if (!data.TryGetValue(name, out clauses))
            {
                clauses = new Clauses(joiner, prefix, postfix);
                data[name] = clauses;
            }

            if (!string.IsNullOrWhiteSpace(uniqueKey) && clauses.Any(x => x.UniqueKey == uniqueKey))
            {
                //join condition is already added, we should not add it again.
                return;
            }

            clauses.Add(new Clause { Sql = sql, Parameters = parameters, IsInclusive = IsInclusive, UniqueKey = uniqueKey });
            seq++;
        }

        public SqlBuilder Intersect(string sql, dynamic parameters = null)
        {
            AddClause("intersect", sql, parameters, joiner: Environment.NewLine + "INTERSECT " + Environment.NewLine, prefix: Environment.NewLine + " ", postfix: Environment.NewLine);
            return this;
        }

        public SqlBuilder InnerJoin(string sql, dynamic parameters = null, string uniqueKey = null)
        {
            AddClause("innerjoin", sql, parameters, joiner: Environment.NewLine + "INNER JOIN ", prefix: Environment.NewLine + "INNER JOIN ", postfix: Environment.NewLine, uniqueKey: uniqueKey);
            return this;
        }

        public SqlBuilder LeftJoin(string sql, dynamic parameters = null, string uniqueKey = null)
        {
            AddClause("leftjoin", sql, parameters, joiner: Environment.NewLine + "LEFT JOIN ", prefix: Environment.NewLine + "LEFT JOIN ", postfix: Environment.NewLine, uniqueKey: uniqueKey);
            return this;
        }

        public SqlBuilder RightJoin(string sql, dynamic parameters = null, string uniqueKey = null)
        {
            AddClause("rightjoin", sql, parameters, joiner: Environment.NewLine + "RIGHT JOIN ", prefix: Environment.NewLine + "RIGHT JOIN ", postfix: Environment.NewLine, uniqueKey: uniqueKey);
            return this;
        }

        public SqlBuilder Where(string sql, dynamic parameters = null)
        {
            AddClause("where", sql, parameters, "AND ", prefix: "WHERE ", postfix: Environment.NewLine);
            return this;
        }

        public SqlBuilder OrWhere(string sql, dynamic parameters = null)
        {
            AddClause("where", sql, parameters, "AND ", prefix: "WHERE ", postfix: Environment.NewLine, IsInclusive: true);
            return this;
        }

        public SqlBuilder OrderBy(string sql, dynamic parameters = null)
        {
            AddClause("orderby", sql, parameters, " , ", prefix: "ORDER BY ", postfix: Environment.NewLine);
            return this;
        }

        public SqlBuilder Select(string sql, dynamic parameters = null)
        {
            AddClause("select", sql, parameters, " , ", prefix: "", postfix: Environment.NewLine);
            return this;
        }

        public SqlBuilder AddParameters(dynamic parameters)
        {
            AddClause("--parameters", "", parameters, "");
            return this;
        }

        public SqlBuilder Join(string sql, dynamic parameters = null, string uniqueKey = null)
        {
            AddClause("join", sql, parameters, joiner: Environment.NewLine + "JOIN ", prefix: Environment.NewLine + "JOIN ", postfix: Environment.NewLine, uniqueKey: uniqueKey);
            return this;
        }

        public SqlBuilder GroupBy(string sql, dynamic parameters = null)
        {
            AddClause("groupby", sql, parameters, joiner: " , ", prefix: Environment.NewLine + "GROUP BY ", postfix: Environment.NewLine);
            return this;
        }

        public SqlBuilder Having(string sql, dynamic parameters = null)
        {
            AddClause("having", sql, parameters, joiner: Environment.NewLine + "AND ", prefix: "HAVING ", postfix: Environment.NewLine);
            return this;
        }
    }
}
