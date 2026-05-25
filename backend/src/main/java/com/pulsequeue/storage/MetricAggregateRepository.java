package com.pulsequeue.storage;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface MetricAggregateRepository extends JpaRepository<MetricAggregateEntity, Long> {

    List<MetricAggregateEntity> findByServiceIdAndWindowStartAfterOrderByWindowStartAsc(
        String serviceId, Instant after
    );

    @Query("SELECT DISTINCT m.serviceId FROM MetricAggregateEntity m " +
           "WHERE m.windowStart > :since")
    List<String> findActiveServiceIds(@Param("since") Instant since);

    @Query("SELECT m FROM MetricAggregateEntity m " +
           "WHERE m.windowStart > :since " +
           "ORDER BY m.windowStart ASC")
    List<MetricAggregateEntity> findAllSince(@Param("since") Instant since);
}
