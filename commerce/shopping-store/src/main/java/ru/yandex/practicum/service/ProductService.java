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
        return productRepository.findByState(ProductState.ACTIVE)
                .stream()
                .map(productMapper::toDto)
                .toList();
    }

    public Page<ProductDto> getProductsPage(Pageable pageable) {
        return productRepository.findByState(ProductState.ACTIVE, pageable)
                .map(productMapper::toDto);
    }

    public ProductDto getProductById(UUID id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found: " + id));
        return productMapper.toDto(product);
    }

    public List<ProductDto> getProductsByCategory(String category) {
        ProductCategory productCategory = ProductCategory.valueOf(category.toUpperCase());
        return productRepository.findByCategoryAndState(productCategory, ProductState.ACTIVE)
                .stream()
                .map(productMapper::toDto)
                .toList();
    }

    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        Product product = productMapper.toEntity(productDto);
        if (product.getState() == null) {
            product.setState(ProductState.ACTIVE);
        }
        Product saved = productRepository.save(product);
        log.info("Created product: {}", saved.getId());
        return productMapper.toDto(saved);
    }

    @Transactional
    public ProductDto updateProduct(UUID id, ProductDto productDto) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found: " + id));
        productMapper.updateEntity(productDto, product);
        Product saved = productRepository.save(product);
        log.info("Updated product: {}", id);
        return productMapper.toDto(saved);
    }

    @Transactional
    public ProductDto deleteProduct(UUID id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found: " + id));
        product.setState(ProductState.DEACTIVATE);
        Product saved = productRepository.save(product);
        log.info("Deactivated product: {}", id);
        return productMapper.toDto(saved);
    }

    @Transactional
    public void activateProduct(UUID id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found: " + id));
        product.setState(ProductState.ACTIVE);
        productRepository.save(product);
        log.info("Activated product: {}", id);
    }

    @Transactional
    public void deactivateProduct(UUID id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found: " + id));
        product.setState(ProductState.DEACTIVATE);
        productRepository.save(product);
        log.info("Deactivated product: {}", id);
    }

    @Transactional
    public ProductDto updateQuantityState(UUID id, String quantityState) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Product not found: " + id));
        product.setQuantityState(ProductQuantityState.valueOf(quantityState.toUpperCase()));
        Product saved = productRepository.save(product);
        log.info("Updated quantity state for product {} to {}", id, quantityState);
        return productMapper.toDto(saved);
    }
}