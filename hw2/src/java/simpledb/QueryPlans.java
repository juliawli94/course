package simpledb;

public class QueryPlans {

	public QueryPlans(){
	}

	//SELECT * FROM T1, T2 WHERE T1.column0 = T2.column0;
	public Operator queryOne(DbIterator t1, DbIterator t2) {
		JoinPredicate joinPredicate = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
		return new Join(joinPredicate, t1, t2);
	}

	//SELECT * FROM T1, T2 WHERE T1. column0 > 1 AND T1.column1 = T2.column1;
	public Operator queryTwo(DbIterator t1, DbIterator t2) {
		JoinPredicate joinPredicate = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
		Join j = new Join(joinPredicate, t1, t2);
		Predicate p = new Predicate(0, Predicate.Op.GREATER_THAN, new IntField(1));
		return new Filter(p, j);
	}

	//SELECT column0, MAX(column1) FROM T1 WHERE column2 > 1 GROUP BY column0;
	public Operator queryThree(DbIterator t1) {
		Predicate pred = new Predicate(2, Predicate.Op.GREATER_THAN, new IntField(1));
		Filter f = new Filter(pred, t1);
		return new Aggregate(f, 1, 0, Aggregator.Op.MAX);
	}

	// SELECT ​​* FROM T1, T2
	// WHERE T1.column0 < (SELECT COUNT(*​​) FROM T3)
	// AND T2.column0 = (SELECT AVG(column0) FROM T3)
	// AND T1.column1 >= T2. column1
	// ORDER BY T1.column0 DESC;
	public Operator queryFour(DbIterator t1, DbIterator t2, DbIterator t3) throws TransactionAbortedException, DbException {
		// first condition
		Aggregate count = new Aggregate(t3, 0, -1, Aggregator.Op.COUNT);
		count.open();
		Field countField = count.fetchNext().getField(0);
		Predicate p1 = new Predicate(0, Predicate.Op.LESS_THAN, countField);
		count.close();
		Filter f1 = new Filter(p1, t1);

		// second condition
		Aggregate avg = new Aggregate(t3, 0, -1, Aggregator.Op.AVG);
		avg.open();
		Field avgField = avg.fetchNext().getField(0);
		Predicate p2 = new Predicate(0, Predicate.Op.EQUALS, avgField);
		avg.close();
		Filter f2 = new Filter(p2, t2);

		// third condition
		JoinPredicate jp = new JoinPredicate(1, Predicate.Op.GREATER_THAN_OR_EQ, 1);
		Join j = new Join(jp, f1, f2);

		// order by
		return new OrderBy(0, false, j);
	}


}