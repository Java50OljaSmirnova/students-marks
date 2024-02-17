package telran.students.service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.TemporalAdjusters;
import java.util.List;

import org.bson.Document;
import org.springframework.data.domain.Sort.*;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import telran.students.dto.Mark;
import telran.students.dto.Student;
import telran.students.dto.StudentAvgScore;
import telran.students.exceptions.StudentIllegalStateException;
import telran.students.exceptions.StudentNotFoundException;
import telran.students.model.StudentDoc;
import telran.students.repo.IdPhone;
import telran.students.repo.StudentRepo;
@Service
@RequiredArgsConstructor
@Slf4j
public class StudentsServiceImpl implements StudentsService {
	final StudentRepo studentRepo;
	final MongoTemplate mongoTemplate;
	@Override
	@Transactional
	public Student addStudent(Student student) {
		long id = student.id();
		if(studentRepo.existsById(id)) {
			log.error("student with id {} already exists", id);
			throw new StudentIllegalStateException();
		}
		StudentDoc studentDoc = new StudentDoc(student);
		studentRepo.save(studentDoc);
		log.debug("student {} has been saved", student);
		return student;
	}

	@Override
	public Mark addMark(long id, Mark mark) {
		StudentDoc studentDoc = studentRepo.findById(id)
				.orElseThrow(() -> new StudentNotFoundException());
		List<Mark> marks = studentDoc.getMarks();
		log.debug("student with id {}, has marks {} before adding new one",
				id, marks);
		marks.add(mark);
		StudentDoc savedStudent = studentRepo.save(studentDoc);
		log.debug("new marks after saving are {}", savedStudent.getMarks());
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
		Student student = getStudent(id);
		studentRepo.deleteById(id);
		log.debug("student {} has been removed", student);
		return student;
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
		List<IdPhone> idPhones = studentRepo.fndeFewMarks(nMarks);
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
		if(!studentRepo.existsById(id)) {
			throw new StudentNotFoundException();
		}
		MatchOperation matchStudentOperation = Aggregation.match(Criteria.where("id").is(id));
		UnwindOperation unwindOperation = Aggregation.unwind("marks");
		MatchOperation matchSubject = Aggregation.match(Criteria.where("marks.subject").is(subject));
		ProjectionOperation projectOperation = Aggregation.project("marks.subject", 
				"marks.score", "marks.date");
		Aggregation pipeline = Aggregation.newAggregation(matchStudentOperation, unwindOperation, 
				matchSubject, projectOperation);
		var aggregationResult = mongoTemplate.aggregate(pipeline, StudentDoc.class, Document.class);
		List<Document> documents = aggregationResult.getMappedResults();
		log.debug("received {} documents", documents.size());
		List<Mark> res = documents.stream().map(d -> new Mark(d.getString("subject"), 
				d.getInteger("score"), d.getDate("date").toInstant()
				.atZone(ZoneId.systemDefault()).toLocalDate())).toList();
		log.debug("marks of subject {} of student {} ar {}", subject, id, res);
		return res;
	}

	@Override
	public List<StudentAvgScore> getStudentAvgScoreGreater(int avgThreshold) {
		UnwindOperation unwindOperation = Aggregation.unwind("marks");
		GroupOperation groupOperation = Aggregation.group("id").avg("marks.score").as("avgScore");
		MatchOperation matchOperation = Aggregation.match(Criteria.where("avgScore")
				.gt(avgThreshold));
		SortOperation sortOperation = Aggregation.sort(Direction.DESC, "avgScore");
		Aggregation pipeline = Aggregation.newAggregation(unwindOperation, groupOperation, matchOperation,
				sortOperation);
		var aggregationResult = mongoTemplate.aggregate(pipeline, StudentDoc.class, Document.class);
		List<Document> documents = aggregationResult.getMappedResults();
		List<StudentAvgScore> res = documents.stream().map(d -> new StudentAvgScore(d.getLong("_id")
				, d.getDouble("avgScore").intValue())).toList();
		log.debug("students with avg scores greater than {} are {}", avgThreshold, res);
		return res;
	}

	@Override
	public List<Student> getStudentsAllGoodMarksSubject(String subject, int thresholdScore) {
		// TODO the same as the method getStudentsAllGoodMarks but for a given subject
		// consider additional condition for "subject" in the query object
		return null;
	}

	@Override
	public List<Student> getStudentsMarksAmountBetween(int min, int max) {
		// TODO get students having amount of marks in the closed range [min, max]
		// consider using operator $and inside $expr object like $expr:{$and:[{....},{...}]
		//{....} - contains the object similar to the query of repository method List<IdPhone> findFewMarks(int nMarks);
		return null;
	}

	@Override
	public List<Mark> getStudentMarksAtDates(long id, LocalDate from, LocalDate to) {
		// TODO gets only marks on the dates in a closed range [from, to]
		// of a given student (the same as getStudentsMarksSubject just different match operation
		// think of DRY (Don't Repeat Yourself)
		return null;
	}

	@Override
	public List<String> getBestStudents(int nStudents) {
		//gets list of a given number of the best students
		//Best students are the ones who have most scores greater than 80
		//consider aggregation method count() instead of avg() that we have used at CW #72
		// and LimitOperation as additional AggregationOperation
		return null;
	}

	@Override
	public List<String> getWorstStudents(int nStudents) {
		// TODO gets list of a given number of the worst students
		//Worst students are the ones who have least sum's of all scores
		//Students who have no scores at all should be considered as worst
		//instead of GroupOperation to apply AggregationExpression
		// (with AccumulatorOperators.Sum) and
		// ProjectionOperation for adding new fields with computed values from AggregationExpression
		return null;
	}
}
