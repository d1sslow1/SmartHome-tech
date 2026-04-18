package ru.yandex.practicum.controller;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.dto.ProductDto;
import ru.yandex.practicum.dto.ProductsPageResponse;
import ru.yandex.practicum.enums.ProductCategory;
import ru.yandex.practicum.enums.ProductAvailability;
import ru.yandex.practicum.service.ProductService;

import java.util.Map;

@RestController
@RequestMapping("/shopping-store")
public class ProductController {

    private final ProductService productService;

    public ProductController(ProductService productService) {
        this.productService = productService;
    }

    @GetMapping("/{id}")
    public ProductDto getProduct(@PathVariable Long id) {
        return productService.getProduct(id);
    }

    @GetMapping
    public ProductsPageResponse getProducts(
            @RequestParam(required = false) ProductCategory category,
            @RequestParam(defaultValue = "0") Integer page,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String[] sort) {

        Sort sorting = Sort.unsorted();
        boolean sorted = false;
        if (sort != null && sort.length > 0) {
            sorted = true;
            String[] sortParts = sort[0].split(",");
            String property = sortParts[0];
            Sort.Direction direction = Sort.Direction.ASC;
            if (sortParts.length > 1 && "DESC".equalsIgnoreCase(sortParts[1])) {
                direction = Sort.Direction.DESC;
            }
            sorting = Sort.by(direction, property);
        }

        Pageable pageable = PageRequest.of(page, size, sorting);
        Page<ProductDto> productPage = productService.getProductsPage(category, pageable);

        return new ProductsPageResponse(
                productPage.getContent(),
                productPage.getNumber(),
                productPage.getSize(),
                productPage.getTotalElements(),
                productPage.getTotalPages(),
                sorted
        );
    }

    @PutMapping
    public ProductDto addOrUpdateProduct(@RequestBody ProductDto product) {
        if (product.getId() == null) {
            return productService.addProduct(product);
        } else {
            return productService.updateProduct(product);
        }
    }

    @PostMapping
    public ProductDto addProductPost(@RequestBody ProductDto product) {
        return productService.addProduct(product);
    }

    @PostMapping("/removeProductFromStore")
    public boolean removeProductFromStore(@RequestBody String productId) {
        try {
            Long id = Long.parseLong(productId.replace("\"", ""));
            productService.deactivateProduct(id);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @PostMapping("/quantityState")
    public boolean setQuantityState(
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) ProductAvailability quantityState,
            @RequestBody(required = false) Map<String, Object> body) {

        if (productId != null && quantityState != null) {
            productService.updateQuantityState(productId, quantityState);
            return true;
        }

        if (body != null && body.containsKey("productId") && body.containsKey("quantityState")) {
            Long id = convertToLong(body.get("productId"));
            ProductAvailability state = convertToAvailability(body.get("quantityState"));
            if (id != null && state != null) {
                productService.updateQuantityState(id, state);
                return true;
            }
        }

        return false;
    }

    private Long convertToLong(Object obj) {
        if (obj instanceof Integer) return ((Integer) obj).longValue();
        if (obj instanceof Long) return (Long) obj;
        if (obj instanceof String) return Long.parseLong((String) obj);
        return null;
    }

    private ProductAvailability convertToAvailability(Object obj) {
        if (obj instanceof String) return ProductAvailability.valueOf((String) obj);
        return null;
    }
}