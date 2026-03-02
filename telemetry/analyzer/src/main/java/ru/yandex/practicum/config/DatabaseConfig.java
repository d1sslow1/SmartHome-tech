package ru.yandex.practicum.config;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableJpaRepositories(basePackages = "ru.yandex.practicum.repository")
@EntityScan(basePackages = "ru.yandex.practicum.model")
@EnableTransactionManagement
public class DatabaseConfig {
}