package ru.yandex.practicum.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.model.Product;

import java.util.List;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {

    Page<Product> findByCategory(ProductCategory category, Pageable pageable);

    List<Product> findByCategory(ProductCategory category);

    Page<Product> findAll(Pageable pageable);
}