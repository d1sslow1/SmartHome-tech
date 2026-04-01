package ru.yandex.practicum.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.dto.ProductCategory;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.dto.ProductQuantityState;
import ru.yandex.practicum.dto.ProductState;
import ru.yandex.practicum.exception.NotFoundException;
import ru.yandex.practicum.mapper.ProductMapper;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.repository.ProductRepository;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {

    private final ProductRepository productRepository;
    private final ProductMapper productMapper;

    public List<ProductDto> getAllActiveProducts() {
        log.debug("Getting all active products");
        return productRepository.findByState(ProductState.ACTIVE)
                .stream()
                .map(productMapper::toDto)
                .toList();
    }

    public Page<ProductDto> getProductsPage(Pageable pageable) {
        log.debug("Getting products page: {}", pageable);
        return productRepository.findByState(ProductState.ACTIVE, pageable)
                .map(productMapper::toDto);
    }

    public Page<ProductDto> getProductsByCategoryPage(String category, Pageable pageable) {
        log.debug("Getting products by category page: {}, {}", category, pageable);
        try {
            ProductCategory productCategory = ProductCategory.valueOf(category.toUpperCase());
            return productRepository.findByCategoryAndState(productCategory, ProductState.ACTIVE, pageable)
                    .map(productMapper::toDto);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid category: {}", category);
            throw new NotFoundException("Invalid category: " + category);
        }
    }

    public ProductDto getProductById(UUID id) {
        log.debug("Getting product by id: {}", id);
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found with id: " + id));
        return productMapper.toDto(product);
    }

    public List<ProductDto> getProductsByCategory(String category) {
        log.debug("Getting products by category: {}", category);
        try {
            ProductCategory productCategory = ProductCategory.valueOf(category.toUpperCase());
            return productRepository.findByCategoryAndState(productCategory, ProductState.ACTIVE)
                    .stream()
                    .map(productMapper::toDto)
                    .toList();
        } catch (IllegalArgumentException e) {
            log.warn("Invalid category: {}", category);
            throw new NotFoundException("Invalid category: " + category);
        }
    }

    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        log.debug("Creating new product: {}", productDto.getProductName());
        Product product = productMapper.toEntity(productDto);
        product.setState(ProductState.ACTIVE);

        Product savedProduct = productRepository.save(product);
        log.info("Created product with id: {}", savedProduct.getId());
        return productMapper.toDto(savedProduct);
    }

    @Transactional
    public ProductDto updateProduct(UUID id, ProductDto productDto) {
        log.debug("Updating product: {}", id);
        Product existingProduct = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found with id: " + id));

        productMapper.updateEntity(productDto, existingProduct);

        Product updatedProduct = productRepository.save(existingProduct);
        log.info("Updated product: {}", id);
        return productMapper.toDto(updatedProduct);
    }

    @Transactional
    public void deleteProduct(UUID id) {
        log.debug("Deactivating product: {}", id);
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found with id: " + id));

        // Мягкое удаление - меняем статус на DEACTIVATE
        product.setState(ProductState.DEACTIVATE);
        productRepository.save(product);
        log.info("Deactivated product: {}", id);
    }

    @Transactional
    public void activateProduct(UUID id) {
        log.debug("Activating product: {}", id);
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found with id: " + id));

        product.setState(ProductState.ACTIVE);
        productRepository.save(product);
        log.info("Activated product: {}", id);
    }

    @Transactional
    public void deactivateProduct(UUID id) {
        log.debug("Deactivating product: {}", id);
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found with id: " + id));

        product.setState(ProductState.DEACTIVATE);
        productRepository.save(product);
        log.info("Deactivated product: {}", id);
    }

    @Transactional
    public void updateQuantityState(UUID id, String quantityState) {
        log.debug("Updating quantity state for product: {} to {}", id, quantityState);
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found with id: " + id));

        try {
            ProductQuantityState state = ProductQuantityState.valueOf(quantityState.toUpperCase());
            product.setQuantityState(state);
            productRepository.save(product);
            log.info("Updated quantity state for product: {} to {}", id, state);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid quantity state: " + quantityState);
        }
    }
}