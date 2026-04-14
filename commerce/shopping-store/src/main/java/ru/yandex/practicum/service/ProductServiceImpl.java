package ru.yandex.practicum.service;

import org.springframework.stereotype.Service;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductStatus;
import ru.yandex.practicum.model.Product;
import ru.yandex.practicum.repository.ProductRepository;

import java.util.List;

@Service
public class ProductServiceImpl implements ProductService {

    private final ProductRepository repository;

    public ProductServiceImpl(ProductRepository repository) {
        this.repository = repository;
    }

    @Override
    public List<ProductDto> getProducts(ProductCategory category) {
        List<Product> products;
        if (category != null) {
            products = repository.findByCategoryAndStatus(category, ProductStatus.ACTIVE);
        } else {
            products = repository.findByStatus(ProductStatus.ACTIVE);
        }
        return products.stream().map(this::toDto).toList();
    }

    @Override
    public ProductDto getProduct(Long id) {
        Product product = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found with id: " + id));
        return toDto(product);
    }

    @Override
    public ProductDto addProduct(ProductDto dto) {
        Product product = new Product();
        product.setName(dto.getName());
        product.setDescription(dto.getDescription());
        product.setCategory(dto.getCategory());
        product.setAvailability(dto.getAvailability());
        product.setImages(dto.getImages());
        product.setStatus(ProductStatus.ACTIVE);
        Product saved = repository.save(product);
        return toDto(saved);
    }

    @Override
    public ProductDto updateProduct(ProductDto dto) {
        if (dto.getId() == null) {
            return addProduct(dto);
        }
        Product existing = repository.findById(dto.getId())
                .orElseThrow(() -> new RuntimeException("Product not found with id: " + dto.getId()));

        existing.setName(dto.getName());
        existing.setDescription(dto.getDescription());
        existing.setCategory(dto.getCategory());
        existing.setAvailability(dto.getAvailability());
        existing.setImages(dto.getImages());
        existing.setStatus(dto.getStatus() != null ? dto.getStatus() : existing.getStatus());

        Product saved = repository.save(existing);
        return toDto(saved);
    }

    @Override
    public void deactivateProduct(Long id) {
        Product product = repository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found with id: " + id));
        product.setStatus(ProductStatus.DEACTIVATE);
        repository.save(product);
    }

    @Override
    public void updateAvailability(Long productId, ProductAvailability availability) {
        Product product = repository.findById(productId)
                .orElseThrow(() -> new RuntimeException("Product not found with id: " + productId));
        product.setAvailability(availability);
        repository.save(product);
    }

    private ProductDto toDto(Product product) {
        ProductDto dto = new ProductDto();
        dto.setId(product.getId());
        dto.setName(product.getName());
        dto.setDescription(product.getDescription());
        dto.setCategory(product.getCategory());
        dto.setAvailability(product.getAvailability());
        dto.setStatus(product.getStatus());
        dto.setImages(product.getImages());
        return dto;
    }
}