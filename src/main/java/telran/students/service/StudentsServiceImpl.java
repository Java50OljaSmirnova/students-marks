package telran.students.service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;
import java.util.List;

import org.bson.Document;
import org.springframework.data.domain.Sort.*;
import org.springframework.data.mongodb.core.*;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.dao.DuplicateKeyException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import telran.students.dto.*;
import telran.students.exceptions.StudentIllegalStateException;
import telran.students.exceptions.StudentNotFoundException;
import telran.students.model.StudentDoc;
import telran.students.repo.*;
@Service
@RequiredArgsConstructor
@Slf4j
public class StudentsServiceImpl implements StudentsService {
	private static final String MARKS_SCORE_FIELD = "marks.score";
	private static final String ID_FIELD = "id";
	private static final String SUM_SCORES_FIELD = "sumScore";
	private static final String MARKS_FIELD = "marks";
	private static final String COUNT_FIELD = "count";
	private static final String ID_DOCUMENT_FIELD = "_id";
	private static final int BEST_STUDENTS_MARK_THRESHOLD = 80;
	private static final String AVG_SCORE_FIELD = "avgScore";
	private static final String MARKS_SUBJECT_FIELD = "marks.subject";
	private static final String MARKS_DATE_FIELD = "marks.date";
	private static final String SCORE_FIELD = "score";
	private static final String DATE_FIELD = "date";
	private static final String SUBJECT_FIELD = "subject";
	final StudentRepo studentRepo;
	final MongoTemplate mongoTemplate;
	FindAndModifyOptions options = new FindAndModifyOptions().returnNew(true).upsert(false);
	
	@Override
	public Student addStudent(Student student) {
		long id = student.id();
		try {
			mongoTemplate.insert(new StudentDoc(student));
		}catch(DuplicateKeyException e) {
			log.error("student with id : {} already exists", id);
			throw new StudentIllegalStateException();
		}
		log.debug("student {} has been added", student);
		return student;
	}

	@Override
	public Mark addMark(long id, Mark mark) {
		Query query = new Query(Criteria.where(ID_FIELD).is(id));
		Update update = new Update();
		update.push(MARKS_FIELD, mark);
		StudentDoc studentDoc = mongoTemplate.findAndModify(query, update, options, StudentDoc.class);
		if(studentDoc == null) {
			log.error("student with id: {} not found", id);
			throw new StudentNotFoundException();
		}
		log.debug("mark {} has been added for student with id {}", mark, id);
		return mark;
	}

	@Override
	@Transactional
	public Student updatePhoneNumber(long id, String phoneNumber) {
		StudentDoc studentDoc = studentRepo.findById(id).orElseThrow(() -> new StudentNotFoundException());
		log.debug("student with id {}, old phone number {}, new phone number {}", id, studentDoc.getPhone(),
				phoneNumber);
		studentDoc.setPhone(phoneNumber);
		Student res = studentRepo.save(studentDoc).build();
		log.debug("student {} has been saved", res);
		return res;
	}

	@Override
	public Student removeStudent(long id) {
		Query query = new Query(Criteria.where(ID_FIELD).is(id));
		StudentDoc studentDoc = mongoTemplate.findAndRemove(query, StudentDoc.class);
		if(studentDoc == null) {
			log.error("student with id {} not found", id);
			throw new StudentNotFoundException();
		}
		log.debug("student with id {} has been removed", id);
		return studentDoc.build();
	}

	@Override
	public Student getStudent(long id) {
		StudentDoc studentDoc = studentRepo.findStudentNoMarks(id);
		if(studentDoc == null) {
			throw new StudentNotFoundException();
		}
		log.debug("marks of found student {}", studentDoc.getMarks());
		Student student = studentDoc.build();
		log.debug("found student {}", student);
		return student;
	}

	@Override
	public List<Mark> getMarks(long id) {
		StudentDoc studentDoc = studentRepo.findStudentOnlyMarks(id);
		if(studentDoc == null) {
			throw new StudentNotFoundException();
		}
		List<Mark> res = studentDoc.getMarks();
		log.debug("phone: {}, id: {}", studentDoc.getPhone(), studentDoc.getId());
		log.debug("marks of found student {}", res);
		return res;
	}
	
	@Override
	public List<Student> getStudentsAllGoodMarks(int markThreshold) {
		List<IdPhone> idPhones = studentRepo.findAllGoodMarks(markThreshold);
		List<Student> res = idPhonesToStudents(idPhones);
		log.debug("students having marks greater than {} are {}", markThreshold, res);
		return res;
	}

	@Override
	public List<Student> getStudentsFewMarks(int nMarks) {
		List<IdPhone> idPhones = studentRepo.findFewMarks(nMarks);
		List<Student> res = idPhonesToStudents(idPhones);
		log.debug("student having amount of marks less than {} are {}", nMarks, res);
		return res;
	}

	@Override
	public Student getStudentByPhoneNumber(String phoneNumber) {
		IdPhone idPhone = studentRepo.findByPhone(phoneNumber);
		Student res = null;
		if(idPhone != null) {
			res = new Student(idPhone.getId(), idPhone.getPhone());
		}
		log.debug("student {}", res);
		return res;
	}

	@Override
	public List<Student> getStudentsByPhonePrefix(String prefix) {
		List<IdPhone> idPhones = studentRepo.findByPhoneRegex(prefix + ".+");
		List<Student> res = idPhonesToStudents(idPhones);
		log.debug("students {}", res);
		return res;
	}

	@Override
	public List<Student> getStudentsMarksDate(LocalDate date) {
		List<IdPhone> idPhones = studentRepo.findByMarksDate(date);
		List<Student> res = idPhonesToStudents(idPhones);
		log.debug("students {}", res);
		return res;
	}

	private List<Student> idPhonesToStudents(List<IdPhone> idPhones) {
		return idPhones.stream().map(ip -> new Student(ip.getId(), ip.getPhone())).toList();
	}

	@Override
	public List<Student> getStudentsMarksMonthYear(int month, int year) {
		LocalDate firstDate = LocalDate.of(year, month, 1);
		LocalDate lastDate = firstDate.with(TemporalAdjusters.lastDayOfMonth());
		List<IdPhone> idPhones = studentRepo.findByMarksDateBetween(firstDate, lastDate);
		List<Student> res = idPhonesToStudents(idPhones);
		log.debug("students {}", res);
		return res;
	}

	@Override
	public List<Student> getStudentsGoodSubjectMark(String subject, int markThreshold) {
		List<IdPhone> idPhones = studentRepo.findByMarksSubjectAndMarksScoreGreaterThan(subject, markThreshold);
		List<Student> res = idPhonesToStudents(idPhones);
		log.debug("students {}", res);
		return res;
	}

	@Override
	public List<Mark> getStudentMarksSubject(long id, String subject) {
		MatchOperation matchSubject = Aggregation.match(Criteria.where(MARKS_SUBJECT_FIELD).is(subject));
		List<Mark> res = getStudentMarks(id, matchSubject);
		log.debug("marks of subject {} of student {} ar {}", subject, id, res);
		return res;
	}

	private List<Mark> getStudentMarks(long id, MatchOperation matchMarks) {
		if(!studentRepo.existsById(id)) {
			throw new StudentNotFoundException();
		}
		MatchOperation matchStudentOperation = Aggregation.match(Criteria.where(ID_FIELD).is(id));
		UnwindOperation unwindOperation = Aggregation.unwind(MARKS_FIELD);
		ProjectionOperation projectOperation = Aggregation.project(MARKS_SUBJECT_FIELD, 
				MARKS_SCORE_FIELD, MARKS_DATE_FIELD);
		Aggregation pipeline = Aggregation.newAggregation(matchStudentOperation, unwindOperation, 
				matchMarks, projectOperation);
		var aggregationResult = mongoTemplate.aggregate(pipeline, StudentDoc.class, Document.class);
		List<Document> documents = aggregationResult.getMappedResults();
		log.debug("received {} documents", documents.size());
		List<Mark> res = documents.stream().map(d -> new Mark(d.getString(SUBJECT_FIELD), 
				d.getInteger(SCORE_FIELD), d.getDate(DATE_FIELD).toInstant()
				.atZone(ZoneId.systemDefault()).toLocalDate())).toList();
		return res;
	}

	@Override
	public List<StudentAvgScore> getStudentAvgScoreGreater(int avgThreshold) {
		UnwindOperation unwindOperation = Aggregation.unwind(MARKS_FIELD);
		GroupOperation groupOperation = Aggregation.group(ID_FIELD).avg(MARKS_SCORE_FIELD).as(AVG_SCORE_FIELD);
		MatchOperation matchOperation = Aggregation.match(Criteria.where(AVG_SCORE_FIELD)
				.gt(avgThreshold));
		SortOperation sortOperation = Aggregation.sort(Direction.DESC, AVG_SCORE_FIELD);
		Aggregation pipeline = Aggregation.newAggregation(unwindOperation, groupOperation, matchOperation,
				sortOperation);
		var aggregationResult = mongoTemplate.aggregate(pipeline, StudentDoc.class, Document.class);
		List<Document> documents = aggregationResult.getMappedResults();
		List<StudentAvgScore> res = documents.stream().map(d -> new StudentAvgScore(d.getLong(ID_DOCUMENT_FIELD)
				, d.getDouble(AVG_SCORE_FIELD).intValue())).toList();
		log.debug("students with avg scores greater than {} are {}", avgThreshold, res);
		return res;
	}

	@Override
	public List<Student> getStudentsAllGoodMarksSubject(String subject, int thresholdScore) {
		List<IdPhone> idPhones = studentRepo.findAllGoodMarksSuject(subject, thresholdScore);
		List<Student> res = idPhonesToStudents(idPhones);
		log.debug("students having score greater than {} on subject {} are {}", thresholdScore, subject, res);
		return res;
	}

	@Override
	public List<Student> getStudentsMarksAmountBetween(int min, int max) {
		List<IdPhone> idPhones = studentRepo.findMarksAmountBetween(min, max);
		List<Student> res = idPhonesToStudents(idPhones);
		log.debug("student having amount of marks in the closed range min {}, max {} are {}", min, max, res);
		return res;
	}

	@Override
	public List<Mark> getStudentMarksAtDates(long id, LocalDate from, LocalDate to) {
		MatchOperation matchDates = Aggregation.match(Criteria.where(MARKS_DATE_FIELD).gte(from)
				.lte(to));
		List<Mark> res = getStudentMarks(id, matchDates);
		log.debug("marks of the student  with id {} on dates[{}-{}] are {}",  id, from, to, res);
		return res;

	}

	@Override
	public List<Long> getBestStudents(int nStudents) {
		UnwindOperation unwindOperation = Aggregation.unwind(MARKS_FIELD);
		MatchOperation matchOperation = Aggregation.match(Criteria.where(MARKS_SCORE_FIELD).gt(BEST_STUDENTS_MARK_THRESHOLD));
		GroupOperation groupOperation = Aggregation.group(ID_FIELD).count().as(COUNT_FIELD);
		SortOperation sortOperation = Aggregation.sort(Direction.DESC, COUNT_FIELD);
		LimitOperation limitOperation = Aggregation.limit(nStudents);
		Aggregation pipeline = Aggregation.newAggregation(unwindOperation,matchOperation, groupOperation,
				sortOperation, limitOperation);
		var aggregationResult = mongoTemplate.aggregate(pipeline, StudentDoc.class, Document.class);
		List<Document> documents = aggregationResult.getMappedResults();
		List<Long> res = documents.stream().map(d -> d.getLong(ID_DOCUMENT_FIELD)).toList();
		log.debug("{} students with most scoresgreater than {} are {}", nStudents, BEST_STUDENTS_MARK_THRESHOLD, res);;
		return res;
	}

	@Override
	public List<Long> getWorstStudents(int nStudents) {
		 AggregationExpression agExpres = AccumulatorOperators.Sum.sumOf(MARKS_SCORE_FIELD);
		 ProjectionOperation projectOperation = Aggregation.project(ID_FIELD)
		            .and(agExpres).as(SUM_SCORES_FIELD);
		 SortOperation sortOperation = Aggregation.sort(Direction.ASC, SUM_SCORES_FIELD);
		 LimitOperation limitOperation = Aggregation.limit(nStudents);
		 Aggregation pipeline = Aggregation.newAggregation(projectOperation,
		            sortOperation, limitOperation);
		 var aggregationResult = mongoTemplate.aggregate(pipeline, StudentDoc.class, Document.class);
		 List<Document> documents = aggregationResult.getMappedResults();
	    List<Long> res = documents.stream().map(d -> d.getLong(ID_DOCUMENT_FIELD)).toList();
	    log.debug("{} worst students are {}", nStudents, res);
	    return res;
	
	}
}
